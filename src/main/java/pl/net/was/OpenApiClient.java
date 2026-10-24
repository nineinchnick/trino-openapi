/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import pl.net.was.cache.CachingHttpClient;
import pl.net.was.cache.HttpCache;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.HeaderNames.USER_AGENT;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.ceilDiv;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiClient
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
    // request and response bodies can contain credentials and personal data, so they're logged only when this logger,
    // which is not a child of the connector's package, is enabled on purpose
    private static final Logger bodyLog = Logger.get("trino-openapi-bodies");
    // small decimals would otherwise be written in the scientific notation, like 1E-8, which some servers can't parse
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            // doubles can't hold all values of DECIMAL columns, so keep the exact value of every number with a fraction
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();
    public static final HeaderName OPENAPI_PATH_HEADER = HeaderName.of("X-Trino-OpenAPI-Path");
    public static final String USER_AGENT_VALUE = "trino-openapi/" + OpenApiClient.class.getPackage().getImplementationVersion();
    // OWASP's recommendation for PBKDF2-HMAC-SHA256
    private static final int TOKEN_HASH_ITERATIONS = 210_000;

    private final URI baseUri;

    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;
    private final RateLimiter rateLimiter;
    private final RetryPolicy retryPolicy;

    @Inject
    public OpenApiClient(OpenApiConfig config, CatalogName catalogName, @ForOpenApi HttpClient httpClient, OpenApiSpec openApiSpec)
    {
        this.baseUri = config.getBaseUri();
        // the cache is wrapped by decompression, so it stores compressed responses
        this.httpClient = new DecompressingHttpClient(config.getHttpCacheDirectory()
                .<HttpClient>map(directory -> new CachingHttpClient(
                        httpClient,
                        new HttpCache(getHttpCacheDirectory(config, catalogName, directory), config.getHttpCacheMaxSize(), config.getHttpCacheMaxEntrySize()),
                        Clock.systemUTC()))
                .orElse(httpClient));
        this.openApiSpec = openApiSpec;
        if (config.getMaxRequestsPerSecond() != Double.MAX_VALUE) {
            rateLimiter = RateLimiter.create(config.getMaxRequestsPerSecond());
        }
        else {
            rateLimiter = null;
        }
        this.retryPolicy = RetryPolicy.fromConfig(config, openApiSpec.getRateLimitHeaders());
    }

    public Iterable<List<?>> getRows(OpenApiTableHandle table, int firstPage, int pageIncrement, boolean revalidateCachedResponses)
    {
        List<HttpPath> selectPaths = table.getSelectPaths();
        checkState(!selectPaths.isEmpty(), "paths are empty");
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table, selectPaths, column -> getFilter(column, table.getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        Optional<OpenApiColumn> pageColumn = getUnfilteredPageColumn(table, httpPath);
        Predicate<List<?>> rowFilter = getRowFilter(table);
        if (pageColumn.isEmpty()) {
            return Streams.stream(makeRequest(table, httpPath, pathWithParams.getValue(), getParameterValues(table, httpPath), revalidateCachedResponses, getBodyGenerator(table, httpPath), getResponseHandler(table, httpPath)).rows())
                    .filter(rowFilter)
                    .toList();
        }
        Optional<OpenApiTableHandle> sortedTable = table.getSortItem().flatMap(sortItem -> getSortedTable(table, httpPath, sortItem));
        OpenApiTableHandle pagedTable = sortedTable.orElse(table);
        // without the sort parameters, the first rows are not the top ones, so all pages must be read
        OptionalLong queryLimit = table.getSortItem().isPresent() && sortedTable.isEmpty() ? OptionalLong.empty() : table.getLimit();
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        return pageIterator(
                page -> getPage(withPage(pagedTable, pageColumn.get(), page), httpPath, pathWithParams.getValue(), revalidateCachedResponses),
                firstPage - 1,
                queryLimit.orElse(Long.MAX_VALUE),
                // pages are requested until the previous ones have fewer results than this, so a page that would start past it is never requested
                pagination.maxResults().orElse(Long.MAX_VALUE),
                pageIncrement,
                getPageSize(table, httpPath),
                pagination.maxPages(),
                rowFilter);
    }

    private Predicate<List<?>> getRowFilter(OpenApiTableHandle table)
    {
        Map<ColumnHandle, Domain> domains = table.getRowFilter().getDomains().orElseThrow();
        if (domains.isEmpty()) {
            return _ -> true;
        }
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        List<Type> types = columns.stream()
                .map(OpenApiColumn::getType)
                .toList();
        Map<Integer, Domain> domainsByField = domains.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> columns.stream().map(OpenApiColumn::getHandle).toList().indexOf((OpenApiColumnHandle) entry.getKey()),
                        Map.Entry::getValue));
        return row -> {
            // read values the same way as the record set Trino reads rows from, so they're compared like Trino would compare them
            RecordCursor cursor = new InMemoryRecordSet(types, List.of(row)).cursor();
            checkState(cursor.advanceNextPosition(), "row is missing");
            return domainsByField.entrySet().stream()
                    .allMatch(entry -> entry.getValue().includesNullableValue(readNativeValue(cursor, entry.getKey(), types.get(entry.getKey()))));
        };
    }

    private static Object readNativeValue(RecordCursor cursor, int field, Type type)
    {
        if (cursor.isNull(field)) {
            return null;
        }
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) {
            return cursor.getLong(field);
        }
        if (javaType == double.class) {
            return cursor.getDouble(field);
        }
        if (javaType == boolean.class) {
            return cursor.getBoolean(field);
        }
        if (javaType == Slice.class) {
            return cursor.getSlice(field);
        }
        return cursor.getObject(field);
    }

    private Optional<OpenApiTableHandle> getSortedTable(OpenApiTableHandle table, HttpPath httpPath, SortItem sortItem)
    {
        Optional<Sorting> sorting = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()).sorting();
        if (sorting.isEmpty()) {
            return Optional.empty();
        }
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        String sortValue = columns.stream()
                .filter(column -> column.getName().equals(sortItem.getName()))
                .findFirst()
                .map(column -> sorting.get().sortValues().get(column.getSourceName()))
                .orElse(null);
        if (sortValue == null) {
            return Optional.empty();
        }
        Map<String, String> paramValues = Map.of(
                sorting.get().sortParam(), sortValue,
                sorting.get().directionParam(), sorting.get().directionValue(sortItem.getSortOrder().isAscending()));
        Map<ColumnHandle, NullableValue> sortConstraint = new HashMap<>();
        for (Map.Entry<String, String> paramValue : paramValues.entrySet()) {
            Optional<OpenApiColumn> paramColumn = columns.stream()
                    .filter(column -> paramValue.getKey().equals(column.getSourceName()) && isPredicate(column, httpPath, null))
                    .findFirst();
            if (paramColumn.isEmpty() || !(paramColumn.get().getType() instanceof VarcharType)) {
                return Optional.empty();
            }
            Object filter = getFilter(paramColumn.get(), table.getConstraint());
            // a filter on a sort parameter is sent as is, so the server sorts the rows as the filter says
            if (filter != null && !filter.equals(paramValue.getValue())) {
                return Optional.empty();
            }
            sortConstraint.put(paramColumn.get().getHandle(), NullableValue.of(paramColumn.get().getType(), utf8Slice(paramValue.getValue())));
        }
        TupleDomain<ColumnHandle> constraint = TupleDomain.fromFixedValues(sortConstraint);
        return Optional.of(table.cloneWithConstraint(table.getConstraint().isNone() ? constraint : table.getConstraint().intersect(constraint)));
    }

    public boolean canReadPagesWithStride(OpenApiTableHandle table)
    {
        HttpPath httpPath = selectPath(table, table.getSelectPaths(), column -> getFilter(column, table.getConstraint())).getKey();
        if (getUnfilteredPageColumn(table, httpPath).isEmpty()) {
            return false;
        }
        // servers often return the last page for every page number past it, so only a total tells a split that it started past the last page
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        boolean hasTotalPages = !JsonPointer.empty().equals(pagination.totalPagesPointer());
        if (!hasTotalPages && JsonPointer.empty().equals(pagination.totalResultsPointer())) {
            return false;
        }
        // a split can't tell the page size from the pages it reads, because the first one can be the last page, returned again
        return (hasTotalPages && pagination.maxResults().isEmpty()) || getPageSize(table, httpPath).isPresent();
    }

    public Optional<OpenApiTableHandle> getFirstPageWithTotals(OpenApiTableHandle table)
    {
        HttpPath httpPath = selectPath(table, table.getSelectPaths(), column -> getFilter(column, table.getConstraint())).getKey();
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        if (JsonPointer.empty().equals(pagination.totalResultsPointer()) && JsonPointer.empty().equals(pagination.totalPagesPointer())) {
            return Optional.empty();
        }
        return getUnfilteredPageColumn(table, httpPath)
                .map(pageColumn -> withPage(table, pageColumn, 1));
    }

    public List<OpenApiSplit> getSplitsAfterFirstPage(OpenApiTableHandle table, ProbeLimit probeLimit, boolean revalidateCachedResponses)
    {
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table, table.getSelectPaths(), column -> getFilter(column, table.getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        OpenApiColumn pageColumn = getUnfilteredPageColumn(table, httpPath).orElseThrow();
        OptionalLong pageSize = getPageSize(table, httpPath);
        Optional<OpenApiColumn> limitColumn = getLimitColumn(table, httpPath);
        OptionalLong pageCount;
        long serverPageSize;
        // the number of pages can be calculated from the total number of results of a smaller page only if the page size is known,
        // and the total number of pages would be calculated for the smaller page
        if (probeLimit.limit().isPresent() && limitColumn.isPresent() && pageSize.isPresent() && !JsonPointer.empty().equals(pagination.totalResultsPointer())) {
            OpenApiTableHandle probeTable = withLimit(withPage(table, pageColumn, 1), limitColumn.get(), probeLimit.limit().orElseThrow());
            ResultsPage probe = getPage(probeTable, httpPath, pathWithParams.getValue(), revalidateCachedResponses);
            serverPageSize = pageSize.orElseThrow();
            pageCount = probe.totalResults().isPresent() && serverPageSize > 0
                    ? OptionalLong.of(ceilDiv(probe.totalResults().orElseThrow(), serverPageSize))
                    : OptionalLong.empty();
        }
        else {
            // the first page is also read by its own split, because rows can't be sent in splits
            ResultsPage firstPage = getPage(withPage(table, pageColumn, 1), httpPath, pathWithParams.getValue(), revalidateCachedResponses);
            serverPageSize = Iterables.size(firstPage.rows());
            pageCount = getPageCount(firstPage, pageSize);
        }
        if (pageCount.isEmpty()) {
            return List.of(new OpenApiSplit(table, 2, 1));
        }
        long lastPage = pagination.maxPages().isPresent()
                ? min(pageCount.orElseThrow(), pagination.maxPages().orElseThrow())
                : pageCount.orElseThrow();
        // pages starting past the maximum number of results are never requested
        if (pagination.maxResults().isPresent() && serverPageSize > 0) {
            lastPage = min(lastPage, ceilDiv(pagination.maxResults().orElseThrow(), serverPageSize));
        }
        return LongStream.rangeClosed(2, lastPage)
                .mapToObj(page -> new OpenApiSplit(withPage(table, pageColumn, page)))
                .collect(toImmutableList());
    }

    private static OptionalLong getPageCount(ResultsPage firstPage, OptionalLong pageSize)
    {
        long firstPageSize = Iterables.size(firstPage.rows());
        if (firstPage.hasMore().isPresent() && !firstPage.hasMore().get()) {
            return OptionalLong.of(1);
        }
        if (!firstPage.hasLastPageSignal()) {
            return pageSize.isPresent() && firstPageSize < pageSize.orElseThrow() ? OptionalLong.of(1) : OptionalLong.empty();
        }
        OptionalLong pageCountFromResults = OptionalLong.empty();
        if (firstPage.totalResults().isPresent()) {
            long totalResults = firstPage.totalResults().orElseThrow();
            if (totalResults <= firstPageSize) {
                pageCountFromResults = OptionalLong.of(1);
            }
            // more results follow, so the first page is full, and its size is the page size the server uses, even if it's not the declared one
            else if (firstPageSize > 0) {
                pageCountFromResults = OptionalLong.of(ceilDiv(totalResults, firstPageSize));
            }
        }
        return Stream.of(firstPage.totalPages(), pageCountFromResults)
                .flatMapToLong(OptionalLong::stream)
                .min();
    }

    private Optional<OpenApiColumn> getUnfilteredPageColumn(OpenApiTableHandle table, HttpPath httpPath)
    {
        return openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(OpenApiColumn::isPageNumber)
                .filter(column -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath))
                .findFirst()
                .filter(column -> getFilter(column, table.getConstraint()) == null);
    }

    private static OpenApiTableHandle withPage(OpenApiTableHandle table, OpenApiColumn pageColumn, long page)
    {
        TupleDomain<ColumnHandle> pageConstraint = TupleDomain.fromFixedValues(Map.of(
                pageColumn.getHandle(), NullableValue.of(pageColumn.getType(), page)));
        return table.cloneWithConstraint(table.getConstraint().isNone() ? pageConstraint : table.getConstraint().intersect(pageConstraint));
    }

    private static OpenApiTableHandle withLimit(OpenApiTableHandle table, OpenApiColumn limitColumn, long limit)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>(table.getConstraint().getDomains().orElse(Map.of()));
        domains.put(limitColumn.getHandle(), Domain.singleValue(limitColumn.getType(), limit));
        return table.cloneWithConstraint(TupleDomain.withColumnDomains(domains));
    }

    private ResultsPage getPage(OpenApiTableHandle pageTable, HttpPath httpPath, Map<OpenApiColumn, Object> pathParameters, boolean revalidateCachedResponses)
    {
        // the body must be serialized for every page, since it can contain the page number,
        // and the response handler copies the page number to every row
        return makeRequest(pageTable, httpPath, pathParameters, getParameterValues(pageTable, httpPath), revalidateCachedResponses, getBodyGenerator(pageTable, httpPath), getResponseHandler(pageTable, httpPath));
    }

    private OptionalLong getPageSize(OpenApiTableHandle table, HttpPath httpPath)
    {
        Optional<OpenApiColumn> limitColumn = getLimitColumn(table, httpPath);
        if (limitColumn.isEmpty()) {
            return OptionalLong.empty();
        }
        Schema<?> schema = limitColumn.get().getSourceType();
        Object limit = getFilter(limitColumn.get(), table.getConstraint());
        if (limit == null) {
            limit = getParameterDefault(limitColumn.get(), httpPath);
        }
        if (limit == null) {
            limit = schema.getDefault();
        }
        if (!(limit instanceof Number limitNumber)) {
            return OptionalLong.empty();
        }
        // servers often return fewer results than requested when the limit is too high, so it must not exceed the declared maximum
        if (schema.getMaximum() != null) {
            return OptionalLong.of(min(limitNumber.longValue(), schema.getMaximum().longValue()));
        }
        return OptionalLong.of(limitNumber.longValue());
    }

    private Optional<OpenApiColumn> getLimitColumn(OpenApiTableHandle table, HttpPath httpPath)
    {
        Optional<String> limitParam = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()).limitParam();
        if (limitParam.isEmpty()) {
            return Optional.empty();
        }
        return openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                // the row ID column has no source name
                .filter(column -> limitParam.get().equals(column.getSourceName()) && isPredicate(column, httpPath, null))
                .findFirst();
    }

    private JsonResponseHandler getResponseHandler(OpenApiTableHandle table, HttpPath httpPath)
    {
        return new JsonResponseHandler(
                table,
                httpPath,
                openApiSpec.getResultsPointers(table.getSchemaTableName()).get(httpPath),
                openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()));
    }

    private BodyGenerator getBodyGenerator(OpenApiTableHandle table, HttpPath httpPath)
    {
        if (!hasRequestBody(httpPath.method())) {
            return null;
        }
        try {
            return createStaticBodyGenerator(toBytes(serializeMap(table, httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean hasRequestBody(PathItem.HttpMethod method)
    {
        return method == PathItem.HttpMethod.POST || method == PathItem.HttpMethod.PUT || method == PathItem.HttpMethod.PATCH;
    }

    public void insertRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        writeRows(table, "INSERT", table.getTableHandle().getInsertPaths(), page, position, Optional.empty());
    }

    public void updateRows(OpenApiOutputTableHandle table, Page page, int position, Set<OpenApiColumnHandle> updatedColumns)
    {
        writeRows(table, "UPDATE", table.getTableHandle().getUpdatePaths(), page, position, Optional.of(updatedColumns));
    }

    public void deleteRows(OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        table.getTableHandle().checkHasPaths("DELETE", table.getTableHandle().getDeletePaths());
        // don't have to decode the rowId since it's value is copied from the predicate that's still present in the table handle
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(
                table.getTableHandle(),
                table.getTableHandle().getDeletePaths(),
                column -> getFilter(column, table.getTableHandle().getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        makeRequest(
                table.getTableHandle(),
                httpPath,
                pathWithParams.getValue(),
                // operations marked for DELETE, like a POST, can require filters in the request body
                getBodyGenerator(table.getTableHandle(), httpPath),
                new AnyResponseHandler());
    }

    private void writeRows(OpenApiOutputTableHandle table, String statement, List<HttpPath> paths, Page page, int position, Optional<Set<OpenApiColumnHandle>> updatedColumns)
    {
        table.getTableHandle().checkHasPaths(statement, paths);
        Function<OpenApiColumn, Object> rowValues = column -> getRowValue(table, page, position, column);
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table.getTableHandle(), paths, rowValues);
        HttpPath httpPath = pathWithParams.getKey();
        Function<OpenApiColumn, Object> parameterValues = column -> {
            Object value = rowValues.apply(column);
            if (value != null) {
                return value;
            }
            return getParameterDefault(column, httpPath);
        };
        // a PATCH changes only the fields it includes, and other methods usually replace the whole resource
        ObjectNode body = updatedColumns.isPresent() && httpPath.method() == PathItem.HttpMethod.PATCH
                ? serializeUpdatedColumns(table, httpPath, page, position, updatedColumns.get())
                : serializePage(table, httpPath, page, position);
        try {
            makeRequest(
                    table.getTableHandle(),
                    httpPath,
                    pathWithParams.getValue(),
                    parameterValues,
                    false,
                    createStaticBodyGenerator(toBytes(body)),
                    new AnyResponseHandler());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public <T> T makeRequest(
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        return makeRequest(table, httpPath, pathParameters, getParameterValues(table, httpPath), false, bodyGenerator, responseHandler);
    }

    private Function<OpenApiColumn, Object> getParameterValues(OpenApiTableHandle table, HttpPath httpPath)
    {
        Map<OpenApiColumn, Object> boundValues = RangePushdown.getBoundValues(
                openApiSpec.getTables().get(table.getSchemaTableName().getTableName()),
                httpPath,
                table.getConstraint());
        return column -> {
            Object value = getFilter(column, table.getConstraint());
            if (value != null) {
                return value;
            }
            if (boundValues.containsKey(column)) {
                return toParameterValue(column, boundValues.get(column));
            }
            return getParameterDefault(column, httpPath);
        };
    }

    private static Object getParameterDefault(OpenApiColumn column, HttpPath httpPath)
    {
        Object value = column.getParameterDefaults().get(httpPath);
        if (value == null) {
            return null;
        }
        return toParameterValue(column, value);
    }

    private <T> T makeRequest(
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            Function<OpenApiColumn, Object> valueProvider,
            boolean revalidateCachedResponses,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        String uriPath = httpPath.path();
        for (Map.Entry<OpenApiColumn, Object> entry : pathParameters.entrySet()) {
            OpenApiColumn column = entry.getKey();
            String value = getParameterStyle(column, httpPath, ParameterLocation.PATH).serializePathValue(column.getSourceName(), toJsonNode(column, entry.getValue()));
            if (value.isEmpty()) {
                throw new TrinoException(INVALID_ROW_FILTER, format("Path parameter %s can't be empty", column.getName()));
            }
            uriPath = uriPath.replace(format("{%s}", column.getSourceName()), value);
        }
        URI uri;
        try {
            uri = buildUri(baseUri, uriPath, getQueryParams(table, httpPath, valueProvider));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        Request.Builder builder = new Request.Builder()
                .setMethod(httpPath.method().name())
                .setBodyGenerator(bodyGenerator)
                .setUri(uri)
                // the HTTP client would send custom headers, like API keys, to any host a server redirects to
                .setFollowRedirects(false)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString())
                .addHeader(OPENAPI_PATH_HEADER, httpPath.path());
        getFilterColumnValues(table, httpPath, ParameterLocation.HEADER, valueProvider)
                .forEach((column, value) -> builder.addHeader(
                        HeaderName.of(column.getSourceName()),
                        getParameterStyle(column, httpPath, ParameterLocation.HEADER).serializeHeaderValue(column.getSourceName(), toJsonNode(column, value))));
        String cookies = getFilterColumnValues(table, httpPath, ParameterLocation.COOKIE, valueProvider).entrySet().stream()
                .map(entry -> entry.getKey().getSourceName() + "=" + getParameterStyle(entry.getKey(), httpPath, ParameterLocation.COOKIE)
                        .serializeCookieValue(entry.getKey().getSourceName(), toJsonNode(entry.getKey(), entry.getValue())))
                .collect(joining("; "));
        if (!cookies.isEmpty()) {
            builder.addHeader(COOKIE, cookies);
        }
        if (revalidateCachedResponses) {
            builder.addHeader(CACHE_CONTROL, "no-cache");
        }

        Request request = builder.build();
        log.debug(request.toString());
        return retryPolicy.execute(
                request,
                () -> {
                    // retries are requests too, and servers count them against their rate limits
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    return httpClient.execute(request, responseHandler);
                },
                (failure, attempts, notRetriedReason) -> toTrinoException(table, httpPath, request, failure, attempts, notRetriedReason));
    }

    private TrinoException toTrinoException(OpenApiTableHandle table, HttpPath httpPath, Request request, HttpRequestFailure failure, int attempts, Optional<String> notRetriedReason)
    {
        String operation = openApiSpec.getOperationId(httpPath)
                .map(operationId -> "operation " + operationId)
                .orElse("path " + httpPath.path());
        // the query string can contain filter values and API keys
        String context = format("Request %s %s for %s failed", request.getMethod(), withoutQuery(request.getUri()), operation);
        String attemptsSuffix = attempts > 1 ? format(" after %d attempts", attempts) : "";
        return switch (failure) {
            case HttpRequestFailure.TransportError error -> new TrinoException(
                    OPENAPI_CONNECTION_ERROR,
                    format("%s%s: %s", context, attemptsSuffix, requireNonNullElse(error.getCause().getMessage(), error.getCause().getClass().getName())),
                    error.getCause());
            case HttpRequestFailure.ErrorResponse error -> {
                bodyLog.debug("%s with HTTP %d%s, response body%s: %s", context, error.getStatusCode(), attemptsSuffix, error.isResponseBodyTruncated() ? " (truncated)" : "", error.getResponseBody());
                String message = format("%s with HTTP %d%s%s", context, error.getStatusCode(), attemptsSuffix, describeRedirect(request.getUri(), error));
                JsonPointer errorPointer = openApiSpec.getErrorPointers(table.getSchemaTableName()).get(httpPath);
                // the response body is not included, because it might not be readable, and it might contain sensitive information
                String messageWithError = getErrorMessage(error, errorPointer)
                        .map(errorMessage -> message + ": " + errorMessage)
                        .orElse(message);
                yield new TrinoException(OPENAPI_HTTP_ERROR, notRetriedReason
                        .map(reason -> messageWithError + "; " + reason)
                        .orElse(messageWithError));
            }
        };
    }

    private static String describeRedirect(URI requestUri, HttpRequestFailure.ErrorResponse error)
    {
        if (HttpStatus.familyForStatusCode(error.getStatusCode()) != HttpStatus.Family.REDIRECTION) {
            return "";
        }
        Optional<String> location = error.getHeader("Location");
        if (location.isEmpty()) {
            return "";
        }
        URI target;
        try {
            target = requestUri.resolve(location.get());
        }
        catch (IllegalArgumentException _) {
            return "";
        }
        return format(", redirecting to %s, but redirects are not followed, so credentials are not sent to other hosts", withoutQuery(target));
    }

    private static Optional<String> getErrorMessage(HttpRequestFailure.ErrorResponse error, JsonPointer errorPointer)
    {
        if (errorPointer == null || error.isResponseBodyTruncated()) {
            return Optional.empty();
        }
        try {
            JsonNode errorNode = OBJECT_MAPPER.readTree(error.getResponseBody()).at(errorPointer);
            if (errorNode.isMissingNode()) {
                return Optional.empty();
            }
            return Optional.of(errorNode.toString());
        }
        catch (JsonProcessingException _) {
            return Optional.empty();
        }
    }

    static String withoutQuery(URI uri)
    {
        String port = uri.getPort() == -1 ? "" : ":" + uri.getPort();
        return uri.getScheme() + "://" + uri.getHost() + port + uri.getRawPath();
    }

    private Map.Entry<HttpPath, Map<OpenApiColumn, Object>> selectPath(OpenApiTableHandle table, List<HttpPath> paths, Function<OpenApiColumn, Object> valueProvider)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        Predicate<OpenApiColumn> hasValue = column -> valueProvider.apply(column) != null;
        HttpPath httpPath = PathSelector.selectPath(columns, paths, hasValue)
                .orElseThrow(() -> new TrinoException(INVALID_ROW_FILTER, PathSelector.describeMissingPredicates(columns, paths, hasValue)));
        Map<OpenApiColumn, Object> pathParams = columns.stream()
                .filter(column -> isPredicate(column, httpPath, ParameterLocation.PATH))
                .map(column -> new SimpleEntry<>(column, valueProvider.apply(column)))
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return Map.entry(httpPath, pathParams);
    }

    private static URI buildUri(URI baseUri, String path, List<String> queryParams)
            throws URISyntaxException
    {
        // both the path and query parameters are already percent-encoded, according to their parameter styles,
        // and HttpUriBuilder would decode and encode them again, for example turning encoded slashes into path separators.
        // Joining the paths as text, instead of resolving a reference, makes sure the result can't change the host
        StringBuilder uri = new StringBuilder()
                .append(baseUri.getScheme())
                .append("://")
                .append(baseUri.getRawAuthority())
                .append(CharMatcher.is('/').trimTrailingFrom(nullToEmpty(baseUri.getRawPath())))
                .append(path);
        List<String> query = new ArrayList<>();
        if (baseUri.getRawQuery() != null) {
            query.add(baseUri.getRawQuery());
        }
        query.addAll(queryParams);
        if (!query.isEmpty()) {
            uri.append('?').append(String.join("&", query));
        }
        return new URI(uri.toString());
    }

    private List<String> getQueryParams(OpenApiTableHandle table, HttpPath httpPath, Function<OpenApiColumn, Object> valueProvider)
    {
        return getFilterColumnValues(table, httpPath, ParameterLocation.QUERY, valueProvider).entrySet().stream()
                .flatMap(entry -> getParameterStyle(entry.getKey(), httpPath, ParameterLocation.QUERY)
                        .serializeQueryParameter(entry.getKey().getSourceName(), toJsonNode(entry.getKey(), entry.getValue()))
                        .stream())
                .toList();
    }

    private static ParameterStyle getParameterStyle(OpenApiColumn column, HttpPath httpPath, ParameterLocation location)
    {
        return column.getParameterStyles().getOrDefault(httpPath, ParameterStyle.defaultFor(location));
    }

    private static JsonNode toJsonNode(OpenApiColumn column, Object value)
    {
        // values read from pages are already converted, and values read from predicates are native Trino values
        if (value instanceof JsonNode node) {
            return resolveRawValues(node, OpenApiClient::parseRawValue);
        }
        if (value instanceof RawValue rawValue) {
            return parseRawValue(rawValue);
        }
        if (value instanceof Block || value instanceof SqlRow || value instanceof SqlMap) {
            return resolveRawValues((JsonNode) JsonTrinoConverter.convert(writeNativeValue(column.getType(), value), 0, column.getType(), column.getSourceType(), OBJECT_MAPPER), OpenApiClient::parseRawValue);
        }
        return TextNode.valueOf(formatParamValue(value));
    }

    /**
     * Replaces values of union types, which are converted to raw JSON, with parsed JSON nodes,
     * so they're serialized like any other value, instead of as the string representation of RawValue.
     */
    private static JsonNode resolveRawValues(JsonNode node, Function<RawValue, JsonNode> parser)
    {
        if (node instanceof POJONode pojoNode && pojoNode.getPojo() instanceof RawValue rawValue) {
            return parser.apply(rawValue);
        }
        if (node.isArray()) {
            ArrayNode result = OBJECT_MAPPER.createArrayNode();
            node.forEach(element -> result.add(resolveRawValues(element, parser)));
            return result;
        }
        if (node.isObject()) {
            ObjectNode result = OBJECT_MAPPER.createObjectNode();
            node.properties().forEach(field -> result.set(field.getKey(), resolveRawValues(field.getValue(), parser)));
            return result;
        }
        return node;
    }

    private static JsonNode parseRawValue(RawValue rawValue)
    {
        String text = rawValue.rawValue().toString();
        try {
            JsonNode node = OBJECT_MAPPER.readTree(text);
            if (!node.isMissingNode()) {
                return node;
            }
        }
        catch (JsonProcessingException _) {
            // union columns are VARCHAR, so they can also hold plain text, which is sent as-is
        }
        return TextNode.valueOf(text);
    }

    private static String formatParamValue(Object value)
    {
        // BigDecimal.toString() uses the scientific notation for small values
        if (value instanceof BigDecimal decimalValue) {
            return decimalValue.toPlainString();
        }
        return value.toString();
    }

    private Map<String, Object> getFilterValues(OpenApiTableHandle table, HttpPath httpPath, ParameterLocation in)
    {
        return getFilterColumnValues(table, httpPath, in, column -> getFilter(column, table.getConstraint())).entrySet().stream()
                .collect(toMap(entry -> entry.getKey().getSourceName(), Map.Entry::getValue));
    }

    private Map<String, Object> getRecordFilterValues(OpenApiTableHandle table, HttpPath httpPath)
    {
        // records need the Trino native representation of values, not the one used to encode request parameters
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        Map<OpenApiColumn, Object> boundValues = RangePushdown.getBoundValues(columns, httpPath, table.getConstraint());
        return columns.stream()
                .filter(column -> isPredicate(column, httpPath, null))
                .map(column -> new SimpleEntry<>(column.getSourceName(), getNativeRecordValue(column, httpPath, table.getConstraint(), boundValues)))
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Object getNativeRecordValue(OpenApiColumn column, HttpPath httpPath, TupleDomain<ColumnHandle> constraint, Map<OpenApiColumn, Object> boundValues)
    {
        Object value = getNativeFilter(column, constraint);
        if (value != null || boundValues.containsKey(column)) {
            return value;
        }
        // rows read using a default value are the same as rows of a query filtering on it, so they have the same value
        return column.getParameterDefaults().get(httpPath);
    }

    private Map<OpenApiColumn, Object> getFilterColumnValues(OpenApiTableHandle table, HttpPath httpPath, ParameterLocation in, Function<OpenApiColumn, Object> valueProvider)
    {
        checkState(!ParameterLocation.PATH.equals(in), "getFilterValues cannot be used for path parameters");
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        return columns.stream()
                .filter(column -> isPredicate(column, httpPath, in))
                .map(column -> {
                    Object value = valueProvider.apply(column);
                    if (value == null && isRequiredPredicate(column, httpPath, in)) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                    }
                    return new SimpleEntry<>(column, value);
                })
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        return isRequiredPredicate(column, httpPath, in) || isOptionalPredicate(column, httpPath, in);
    }

    private static boolean isRequiredPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        ParameterLocation requiredIn = column.getRequiresPredicate().get(httpPath);
        return requiredIn != null && (in == null || requiredIn.equals(in));
    }

    private static boolean isOptionalPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        ParameterLocation optionalIn = column.getOptionalPredicate().get(httpPath);
        return optionalIn != null && (in == null || optionalIn.equals(in));
    }

    private static Object getRequiredFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        Object value = getFilter(column, constraint);
        if (value == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
        }
        return value;
    }

    private static Object getFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        Object value = getNativeFilter(column, constraint);
        if (value == null) {
            return null;
        }
        return toParameterValue(column, value);
    }

    private static Object toParameterValue(OpenApiColumn column, Object value)
    {
        return switch (column.getType().getBaseName()) {
            case StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.SMALLINT, StandardTypes.TINYINT -> value;
            case StandardTypes.REAL -> intBitsToFloat(((Long) value).intValue());
            case StandardTypes.DOUBLE -> (Double) value;
            case StandardTypes.DECIMAL -> JsonTrinoConverter.toBigDecimal(value, (DecimalType) column.getType());
            case StandardTypes.VARCHAR -> ((Slice) value).toStringUtf8();
            case StandardTypes.DATE -> DateTimeConverter.formatDate((Long) value, column.getSourceType());
            case StandardTypes.TIMESTAMP -> DateTimeConverter.formatTimestamp(value, (TimestampType) column.getType(), column.getSourceType());
            case StandardTypes.BOOLEAN -> value;
            case StandardTypes.MAP -> (SqlMap) value;
            case StandardTypes.ARRAY -> (Block) value;
            case StandardTypes.ROW -> (SqlRow) value;
            default -> throw new TrinoException(INVALID_ROW_FILTER, "Unexpected constraint for " + column.getName() + "(" + column.getType().getBaseName() + ")");
        };
    }

    private static Object getNativeFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column.getHandle());
        }
        if (domain == null || domain.isOnlyNull()) {
            return null;
        }
        return domain.getSingleValue();
    }

    private static Object getRowValue(OpenApiOutputTableHandle table, Page page, int position, OpenApiColumn column)
    {
        Object value = getFilter(column, page, position, table.getColumns().indexOf(column.getHandle()));
        if (value != null) {
            return value;
        }
        // hidden columns are not included in pages, but they could have been used as predicates, like in an UPDATE
        return getFilter(column, table.getTableHandle().getConstraint());
    }

    private static Object getFilter(OpenApiColumn column, Page page, int position, int channel)
    {
        requireNonNull(column, "column is null");
        requireNonNull(page, "page is null");
        if (channel < 0) {
            return null;
        }
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
            return null;
        }
        return JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
    }

    public ObjectNode serializePage(OpenApiOutputTableHandle table, HttpPath httpPath, Page page, int position)
    {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        String tableName = table.getTableHandle().getSchemaTableName().getTableName();
        Map<OpenApiColumnHandle, OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .collect(toImmutableMap(OpenApiColumn::getHandle, identity()));
        for (int channel = 0; channel < table.getColumns().size(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            OpenApiColumn column = columns.get(table.getColumns().get(channel));
            // all fields from a request object param should be marked as predicates
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            Object value = JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            JsonTrinoConverter.putValue(node, column.getSourceName(), parseJsonText(column, value));
        }
        return node;
    }

    private ObjectNode serializeUpdatedColumns(OpenApiOutputTableHandle table, HttpPath httpPath, Page page, int position, Set<OpenApiColumnHandle> updatedColumns)
    {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        String tableName = table.getTableHandle().getSchemaTableName().getTableName();
        Map<OpenApiColumnHandle, OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .collect(toImmutableMap(OpenApiColumn::getHandle, identity()));
        for (int channel = 0; channel < table.getColumns().size(); channel++) {
            OpenApiColumnHandle columnHandle = table.getColumns().get(channel);
            OpenApiColumn column = columns.get(columnHandle);
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            boolean isUpdated = updatedColumns.contains(columnHandle);
            Block block = page.getBlock(channel);
            // required fields, like identifiers, must be sent even if they're not updated
            if (!isUpdated && (block.isNull(position) || !isRequiredPredicate(column, httpPath, ParameterLocation.BODY))) {
                continue;
            }
            // updating a column to NULL sends an explicit null, which clears the field
            Object value = block.isNull(position) ? null : JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            JsonTrinoConverter.putValue(node, column.getSourceName(), parseJsonText(column, value));
        }
        return node;
    }

    public ObjectNode serializeMap(OpenApiTableHandle table, HttpPath httpPath)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);

        Function<OpenApiColumn, Object> parameterValues = getParameterValues(table, httpPath);
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        for (OpenApiColumn column : columns) {
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            Object value = parameterValues.apply(column);
            if (value == null) {
                if (isRequiredPredicate(column, httpPath, ParameterLocation.BODY)) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                }
                continue;
            }
            if (value instanceof Block block) {
                // predicates hold the elements of an array, not a block with the array in its first position
                value = JsonTrinoConverter.convert(writeNativeValue(column.getType(), block), 0, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            }
            else if (value instanceof String stringValue && JsonTrinoConverter.isJsonText(column.getSourceType())) {
                value = new RawValue(stringValue);
            }
            else if (value instanceof SqlRow sqlRow) {
                ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
                JsonTrinoConverter.convertRow(rowNode, sqlRow, (RowType) column.getType(), column.getSourceType(), OBJECT_MAPPER);
                value = rowNode;
            }
            else if (value instanceof SqlMap sqlMap) {
                value = JsonTrinoConverter.convertMap(sqlMap, (MapType) column.getType(), column.getSourceType(), OBJECT_MAPPER);
            }
            JsonTrinoConverter.putValue(node, column.getSourceName(), parseJsonText(column, value));
        }
        return node;
    }

    /**
     * Replaces values of union and unknown types, which are VARCHAR columns holding JSON text, with the parsed JSON,
     * so the request body doesn't include them as strings, or as invalid JSON.
     */
    private static Object parseJsonText(OpenApiColumn column, Object value)
    {
        Function<RawValue, JsonNode> parser = rawValue -> parseJsonValue(column, rawValue);
        if (value instanceof RawValue rawValue) {
            return parser.apply(rawValue);
        }
        if (value instanceof JsonNode node) {
            return resolveRawValues(node, parser);
        }
        return value;
    }

    private static JsonNode parseJsonValue(OpenApiColumn column, RawValue rawValue)
    {
        JsonNode node;
        try {
            node = OBJECT_MAPPER.reader()
                    .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(rawValue.rawValue().toString());
        }
        catch (JsonProcessingException _) {
            // the parser's message quotes the value, which might be sensitive
            node = MissingNode.getInstance();
        }
        // an empty or blank value is parsed into a missing node
        if (node.isMissingNode()) {
            throw new TrinoException(JSON_INPUT_CONVERSION_ERROR, format("Value of column %s is not valid JSON", column.getName()));
        }
        return node;
    }

    private record ResultsPage(Iterable<List<?>> rows, OptionalLong totalResults, OptionalLong totalPages, Optional<Boolean> hasMore)
    {
        // servers can return fewer results than the declared page size, so a short page ends pagination only without these
        boolean hasLastPageSignal()
        {
            return totalResults.isPresent() || totalPages.isPresent() || hasMore.isPresent();
        }
    }

    private class JsonResponseHandler
            implements ResponseHandler<ResultsPage, RuntimeException>
    {
        private final OpenApiTableHandle table;
        private final HttpPath httpPath;
        private final JsonPointer resultsPointer;
        private final Pagination pagination;

        JsonResponseHandler(OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, Pagination pagination)
        {
            this.table = requireNonNull(table, "table is null");
            this.httpPath = requireNonNull(httpPath, "httpPath is null");
            this.resultsPointer = resultsPointer;
            this.pagination = requireNonNull(pagination, "pagination is null");
        }

        @Override
        public ResultsPage handleException(Request request, Exception exception)
        {
            throw new HttpRequestFailure.TransportError(exception);
        }

        @Override
        public ResultsPage handle(Request request, Response response)
        {
            if (response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                return new ResultsPage(List.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
            }
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw HttpRequestFailure.ErrorResponse.read(response);
            }
            String result = readBody(response);
            log.debug("Received response code %d with %d characters", response.getStatusCode(), result.length());
            bodyLog.debug("Received response code %d: %s", response.getStatusCode(), result);

            JsonNode jsonNode;
            try {
                jsonNode = OBJECT_MAPPER.readTree(result);
            }
            catch (JsonProcessingException ex) {
                String message = format("Response to %s is not valid JSON", describe(request));
                // the parser's message quotes the response, which might contain sensitive information, so it's only logged
                bodyLog.debug(ex, message);
                throw new TrinoException(OPENAPI_INVALID_RESPONSE, message);
            }
            return new ResultsPage(
                    convertJson(request, table, httpPath, resultsPointer, jsonNode),
                    getLong(request, jsonNode, "totalResultsPath", pagination.totalResultsPointer()),
                    getLong(request, jsonNode, "totalPagesPath", pagination.totalPagesPointer()),
                    getBoolean(jsonNode, pagination.hasMorePointer()));
        }

        private static OptionalLong getLong(Request request, JsonNode jsonNode, String propertyName, JsonPointer pointer)
        {
            if (JsonPointer.empty().equals(pointer)) {
                return OptionalLong.empty();
            }
            JsonNode valueNode = jsonNode.at(pointer);
            if (valueNode.isMissingNode() || valueNode.isNull()) {
                return OptionalLong.empty();
            }
            if (valueNode.isNumber() && valueNode.canConvertToExactIntegral() && valueNode.canConvertToLong()) {
                return OptionalLong.of(valueNode.asLong());
            }
            // some servers send big numbers as strings, so JavaScript clients don't lose their precision
            Long textValue = valueNode.isTextual() ? Longs.tryParse(valueNode.asText()) : null;
            if (textValue != null) {
                return OptionalLong.of(textValue);
            }
            // the value is not included, because the response might contain sensitive information
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Value of %s %s in the response to %s is not a whole number", propertyName, pointer, describe(request)));
        }

        private static Optional<Boolean> getBoolean(JsonNode jsonNode, JsonPointer pointer)
        {
            if (JsonPointer.empty().equals(pointer)) {
                return Optional.empty();
            }
            JsonNode valueNode = jsonNode.at(pointer);
            if (!valueNode.isBoolean()) {
                return Optional.empty();
            }
            return Optional.of(valueNode.booleanValue());
        }
    }

    private static String readBody(Response response)
    {
        try {
            return new InputStreamReader(response.getInputStream(), UTF_8).readAllAsString();
        }
        catch (IOException e) {
            throw new HttpRequestFailure.TransportError(e);
        }
    }

    private Iterable<List<?>> convertJson(Request request, OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        String rowId = getFilterValues(table, httpPath, null).values().stream().findFirst().map(Object::toString).orElse(null);
        Map<String, Object> recordParams = getRecordFilterValues(table, httpPath);

        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.addAll(convertJsonToRecords(request, table, httpPath, resultsPointer, rowId, recordParams, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.addAll(convertJsonToRecords(request, table, httpPath, resultsPointer, rowId, recordParams, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private Iterable<List<?>> convertJsonToRecords(Request request, OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, String rowId, Map<String, Object> recordParams, JsonNode jsonNode)
    {
        if (!jsonNode.isObject()) {
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response to %s contains a JSON %s where an object was expected", describe(request), jsonNode.getNodeType()));
        }

        Iterable<JsonNode> resultNodes = List.of(jsonNode);
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        if (resultsPointer != null && !JsonPointer.empty().equals(resultsPointer)) {
            resultNodes = getResultNodes(request, jsonNode.at(resultsPointer));
        }

        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (JsonNode resultNode : resultNodes) {
            List<Object> recordBuilder = new ArrayList<>();
            for (OpenApiColumn column : columns) {
                if (column.getName().equals(ROW_ID)) {
                    // TODO this is dangerous, make it configurable and required?
                    recordBuilder.add(rowId);
                    continue;
                }
                String parameterName = column.getSourceName();
                // request parameters are usually not included in the response, so copy them from predicates
                if (isPredicate(column, httpPath, null) && (column.isRequestOnly() || !resultNode.has(parameterName))) {
                    recordBuilder.add(recordParams.getOrDefault(parameterName, null));
                    continue;
                }
                recordBuilder.add(convertToRecordValue(request, column, resultNode.get(parameterName)));
            }
            resultRecordsBuilder.add(recordBuilder);
        }

        return resultRecordsBuilder.build();
    }

    private static Object convertToRecordValue(Request request, OpenApiColumn column, JsonNode value)
    {
        try {
            return JsonTrinoConverter.convertToRecordValue(value, column.getType(), column.getSourceType());
        }
        catch (TrinoException e) {
            if (!e.getErrorCode().equals(OPENAPI_INVALID_RESPONSE.toErrorCode())) {
                throw e;
            }
            throw new TrinoException(
                    () -> e.getErrorCode(),
                    format("Invalid value of column %s in the response to %s: %s", column.getName(), describe(request), e.getMessage()),
                    e);
        }
    }

    private static String describe(Request request)
    {
        return request.getMethod() + " " + withoutQuery(request.getUri());
    }

    private static Iterable<JsonNode> getResultNodes(Request request, JsonNode resultNode)
    {
        if (resultNode instanceof ArrayNode) {
            return resultNode;
        }
        // results can be a union of a single object and an array of objects
        if (resultNode.isObject()) {
            return List.of(resultNode);
        }
        throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Results path in the response to %s points to a JSON %s instead of an array", describe(request), resultNode.getNodeType()));
    }

    private byte[] toBytes(JsonNode rootNode)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OBJECT_MAPPER.createGenerator(baos).writeTree(rootNode);
        byte[] output = baos.toByteArray();
        log.debug("Request body of %d bytes", output.length);
        if (bodyLog.isDebugEnabled()) {
            bodyLog.debug("Request body: %s", new String(output, UTF_8));
        }
        return output;
    }

    private static class AnyResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        @Override
        public Void handleException(Request request, Exception exception)
        {
            throw new HttpRequestFailure.TransportError(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            // servers often respond to writes with 201 Created, 202 Accepted, or 204 No Content
            if (HttpStatus.familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                throw HttpRequestFailure.ErrorResponse.read(response);
            }
            String result = readBody(response);
            log.debug("Received response code %d with %d characters", response.getStatusCode(), result.length());
            bodyLog.debug("Received response code %d: %s", response.getStatusCode(), result);
            return null;
        }
    }

    private Iterable<List<?>> pageIterator(
            IntFunction<ResultsPage> getter,
            int offset,
            long rowLimit,
            long maxResults,
            int pageIncrement,
            OptionalLong pageSize,
            OptionalInt maxPages,
            Predicate<List<?>> rowFilter)
    {
        return () -> new Iterator<>()
        {
            // results are counted before the row filter, because that's what the server counts
            long resultSize;
            long resultLimit = maxResults;
            long rowCount;
            int page = offset + 1;
            boolean isLastPage;
            // servers can return fewer results than the declared page size, so the results before a page are computed from it
            // only by splits reading every few pages, which don't count them
            final OptionalLong stridePageSize = pageIncrement > 1 ? pageSize : OptionalLong.empty();
            Iterator<List<?>> rows;

            @Override
            public boolean hasNext()
            {
                // pages without rows matching the row filter don't end pagination
                while (rows == null || !rows.hasNext()) {
                    if (isLastPage || resultSize >= resultLimit || rowCount >= rowLimit) {
                        return false;
                    }
                    fetchPage();
                }
                return true;
            }

            @Override
            public List<?> next()
            {
                rowCount += 1;
                return rows.next();
            }

            private void fetchPage()
            {
                ResultsPage resultsPage = getter.apply(page);
                int fetchedPage = page;
                page += pageIncrement;
                int pageResults = Iterables.size(resultsPage.rows());
                // splits reading every few pages can start past the last page,
                // and servers might return the last page for every page number past it
                if (isPastLastPage(resultsPage, fetchedPage, stridePageSize, maxResults)) {
                    isLastPage = true;
                    rows = emptyIterator();
                    return;
                }
                if (isPastLastPage(resultsPage, page, stridePageSize, maxResults)) {
                    isLastPage = true;
                }
                // stop on the last page, even if it's not empty, because servers might return the last page for every page number past it
                resultsPage.totalResults().ifPresent(totalResults -> resultLimit = min(resultLimit, totalResults));
                resultSize += pageResults;
                if (pageResults == 0) {
                    isLastPage = true;
                }
                if (!resultsPage.hasLastPageSignal() && pageSize.isPresent() && pageResults < pageSize.orElseThrow()) {
                    isLastPage = true;
                }
                if (resultsPage.hasMore().isPresent() && !resultsPage.hasMore().get()) {
                    isLastPage = true;
                }
                if (maxPages.isPresent() && page > maxPages.orElseThrow()) {
                    isLastPage = true;
                }
                rows = Iterators.filter(resultsPage.rows().iterator(), rowFilter::test);
            }
        };
    }

    private static boolean isPastLastPage(ResultsPage resultsPage, int page, OptionalLong pageSize, long maxResults)
    {
        if (resultsPage.totalPages().isPresent() && page > resultsPage.totalPages().orElseThrow()) {
            return true;
        }
        if (pageSize.isEmpty() || pageSize.orElseThrow() <= 0) {
            return false;
        }
        long firstResult = (page - 1) * pageSize.orElseThrow();
        return firstResult >= maxResults || (resultsPage.totalResults().isPresent() && firstResult >= resultsPage.totalResults().orElseThrow());
    }

    // responses depend on the server and the account, so a catalog created again with the same name, but another base URI or another account,
    // can't reuse responses of the previous one; passwords and client secrets are left out, because the username or client ID already identify the account
    private static Path getHttpCacheDirectory(OpenApiConfig config, CatalogName catalogName, String directory)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        Stream.concat(
                        Stream.of(
                                config.getBaseUri().toString(),
                                config.getAuthenticationType().name(),
                                config.getAuthenticationScheme().name(),
                                config.getUsername(),
                                config.getClientId(),
                                config.getApiKeyName()),
                        new TreeMap<>(config.getApiKeys()).keySet().stream())
                .forEach(value -> hasher.putString(nullToEmpty(value), UTF_8).putByte((byte) 0));
        hasher.putBytes(hashTokens(config));
        return Path.of(directory, catalogName + "-" + hasher.hash());
    }

    // bearer tokens and API key values are the only identity of their accounts, and the directory name is visible to other users,
    // so they're hashed with a slow function, which makes guessing them from the name impractical
    private static byte[] hashTokens(OpenApiConfig config)
    {
        String tokens = Stream.concat(
                        Stream.of(config.getBearerToken(), config.getApiKeyValue()),
                        new TreeMap<>(config.getApiKeys()).values().stream())
                .map(Strings::nullToEmpty)
                .collect(joining("\0"));
        if (CharMatcher.is('\0').matchesAllOf(tokens)) {
            return new byte[0];
        }
        PBEKeySpec keySpec = new PBEKeySpec(tokens.toCharArray(), config.getBaseUri().toString().getBytes(UTF_8), TOKEN_HASH_ITERATIONS, 256);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(keySpec).getEncoded();
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to hash the credentials for the HTTP cache directory name", e);
        }
        finally {
            keySpec.clearPassword();
        }
    }
}
