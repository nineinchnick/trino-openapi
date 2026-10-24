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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.FormDataBodyBuilder;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
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
import pl.net.was.authentication.AuthenticatingHttpClient;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.CatalogCredentials;
import pl.net.was.authentication.ExtraCredentialProvider;
import pl.net.was.authentication.UserCredentials;
import pl.net.was.cache.CachingHttpClient;
import pl.net.was.cache.HttpCache;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.LINK;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.HeaderNames.USER_AGENT;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
    private static final int MAX_HTTP_CACHE_PARTITIONS = 1000;
    private static final Duration HTTP_CACHE_PARTITION_EXPIRATION = Duration.ofHours(1);

    private final OpenApiServers servers;

    private final HttpClient httpClient;
    private final Authentication authentication;
    private final DataSize maxResponseSize;
    private final ExtraCredentialProvider extraCredentialProvider;
    private final Optional<HttpCache> httpCache;
    private final String serverUris;
    // hashing credentials with a slow function for every request would make it slower than the request
    private final Cache<HashCode, String> httpCachePartitions = EvictableCacheBuilder.newBuilder()
            .maximumSize(MAX_HTTP_CACHE_PARTITIONS)
            .expireAfterWrite(HTTP_CACHE_PARTITION_EXPIRATION)
            .build();
    private final OpenApiSpec openApiSpec;
    private final RateLimiter rateLimiter;
    private final RetryPolicy retryPolicy;

    @Inject
    public OpenApiClient(
            OpenApiConfig config,
            CatalogName catalogName,
            @ForUnauthenticatedOpenApi HttpClient httpClient,
            Authentication authentication,
            CatalogCredentials catalogCredentials,
            ExtraCredentialProvider extraCredentialProvider,
            OpenApiSpec openApiSpec,
            OpenApiServers servers)
    {
        requireNonNull(catalogCredentials, "catalogCredentials is null");
        this.servers = servers;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.maxResponseSize = config.getMaxResponseSize();
        this.extraCredentialProvider = requireNonNull(extraCredentialProvider, "extraCredentialProvider is null");
        this.serverUris = getServerUris(servers);
        this.httpCache = config.getHttpCacheDirectory()
                .map(directory -> new HttpCache(getHttpCacheDirectory(catalogCredentials, servers, catalogName, directory), config.getHttpCacheMaxSize(), config.getHttpCacheMaxEntrySize()));
        this.openApiSpec = openApiSpec;
        this.rateLimiter = config.getMaxRequestsPerSecond().map(RateLimiter::create).orElse(null);
        this.retryPolicy = new RetryPolicy(
                new RetryOptions(config.getMaxRetries(), config.getRetryInitialDelay().toJavaTime(), config.getRetryMaxDelay().toJavaTime()),
                openApiSpec.getRateLimitHeaders());
    }

    public Iterable<List<?>> getRows(ConnectorSession session, OpenApiSplit split, boolean revalidateCachedResponses)
    {
        UserCredentials credentials = extraCredentialProvider.getUserCredentials(session.getIdentity());
        OpenApiTableHandle table = split.getTableHandle();
        List<HttpPath> selectPaths = table.getSelectPaths();
        checkState(!selectPaths.isEmpty(), "paths are empty");
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table, selectPaths, column -> getFilter(column, table.getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        Optional<OpenApiColumn> pageColumn = getUnfilteredPageColumn(table, httpPath);
        Optional<OpenApiColumn> cursorColumn = getUnfilteredColumn(table, httpPath, pagination.cursorParam());
        Optional<OpenApiColumn> offsetColumn = getUnfilteredColumn(table, httpPath, pagination.offsetParam());
        // without a cursor parameter, the link is the whole URI of the next page
        boolean followsLinks = pagination.nextPageLinkHeader() && pagination.cursorParam().isEmpty();
        Predicate<List<?>> rowFilter = getRowFilter(table);
        // retries are sent while sending a single request, so they count only against max-requests-per-second, not the split's share of the scan's rate
        Runnable acquireSplitRequest = split.getRequestRate()
                .map(SplitRateLimiter::new)
                .<Runnable>map(limiter -> limiter::acquire)
                .orElse(() -> {});
        if (pageColumn.isEmpty() && cursorColumn.isEmpty() && offsetColumn.isEmpty() && !followsLinks) {
            acquireSplitRequest.run();
            return Streams.stream(makeRequest(credentials, table, httpPath, pathWithParams.getValue(), getParameterValues(table, httpPath), revalidateCachedResponses, getBodyGenerator(table, httpPath), getResponseHandler(table, httpPath, isNotFoundEmpty(httpPath, pathWithParams.getValue()))).rows())
                    .filter(rowFilter)
                    .toList();
        }
        Optional<OpenApiTableHandle> sortedTable = table.getSortItem().flatMap(sortItem -> getSortedTable(table, httpPath, sortItem));
        OpenApiTableHandle pagedTable = sortedTable.orElse(table);
        // without the sort parameters, the first rows are not the top ones, so all pages must be read
        OptionalLong queryLimit = table.getSortItem().isPresent() && sortedTable.isEmpty() ? OptionalLong.empty() : table.getLimit();
        if (cursorColumn.isPresent() || followsLinks) {
            return cursorIterator(
                    next -> getNextPage(credentials, pagedTable, httpPath, pathWithParams.getValue(), cursorColumn, next, revalidateCachedResponses),
                    queryLimit.orElse(Long.MAX_VALUE),
                    pagination.maxResults().orElse(Long.MAX_VALUE),
                    pagination.maxPages(),
                    rowFilter,
                    acquireSplitRequest);
        }
        // without a page number, a page starts after the results the server returned on the previous pages,
        // so they're read one after another, in a single split, because servers can return fewer results than requested
        PageGetter pageGetter = offsetColumn.isPresent()
                ? (page, readResults) -> getPage(credentials, withPage(pagedTable, offsetColumn.get(), readResults), httpPath, pathWithParams.getValue(), page == 1, revalidateCachedResponses)
                : (page, _) -> getPage(credentials, withPage(pagedTable, pageColumn.get(), page), httpPath, pathWithParams.getValue(), page == 1, revalidateCachedResponses);
        return pageIterator(
                pageGetter,
                split.getFirstPage() - 1,
                queryLimit.orElse(Long.MAX_VALUE),
                // pages are requested until the previous ones have fewer results than this, so a page that would start past it is never requested
                pagination.maxResults().orElse(Long.MAX_VALUE),
                split.getPageIncrement(),
                getPageSize(table, httpPath),
                pagination.maxPages(),
                rowFilter,
                acquireSplitRequest);
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
        boolean hasTotalPages = pagination.totalPagesPath().isPresent();
        if (!hasTotalPages && pagination.totalResultsPath().isEmpty()) {
            return false;
        }
        // a split can't tell the page size from the pages it reads, because the first one can be the last page, returned again
        return (hasTotalPages && pagination.maxResults().isEmpty()) || getPageSize(table, httpPath).isPresent();
    }

    public Optional<OpenApiTableHandle> getFirstPageWithTotals(OpenApiTableHandle table)
    {
        HttpPath httpPath = selectPath(table, table.getSelectPaths(), column -> getFilter(column, table.getConstraint())).getKey();
        Pagination pagination = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none());
        if (pagination.totalResultsPath().isEmpty() && pagination.totalPagesPath().isEmpty()) {
            return Optional.empty();
        }
        return getUnfilteredPageColumn(table, httpPath)
                .map(pageColumn -> withPage(table, pageColumn, 1));
    }

    public List<OpenApiSplit> getSplitsAfterFirstPage(ConnectorSession session, OpenApiTableHandle table, ProbeLimit probeLimit, boolean revalidateCachedResponses)
    {
        UserCredentials credentials = extraCredentialProvider.getUserCredentials(session.getIdentity());
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
        if (probeLimit.limit().isPresent() && limitColumn.isPresent() && pageSize.isPresent() && pagination.totalResultsPath().isPresent()) {
            OpenApiTableHandle probeTable = withLimit(withPage(table, pageColumn, 1), limitColumn.get(), probeLimit.limit().orElseThrow());
            ResultsPage probe = getPage(credentials, probeTable, httpPath, pathWithParams.getValue(), true, revalidateCachedResponses);
            serverPageSize = pageSize.orElseThrow();
            pageCount = probe.totalResults().isPresent() && serverPageSize > 0
                    ? OptionalLong.of(ceilDiv(probe.totalResults().orElseThrow(), serverPageSize))
                    : OptionalLong.empty();
        }
        else {
            // the first page is also read by its own split, because rows can't be sent in splits
            ResultsPage firstPage = getPage(credentials, withPage(table, pageColumn, 1), httpPath, pathWithParams.getValue(), true, revalidateCachedResponses);
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

    private Optional<OpenApiColumn> getUnfilteredColumn(OpenApiTableHandle table, HttpPath httpPath, Optional<String> parameterName)
    {
        if (parameterName.isEmpty()) {
            return Optional.empty();
        }
        return openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(column -> parameterName.get().equals(column.getSourceName()) && isPredicate(column, httpPath, null))
                .findFirst()
                .filter(column -> getFilter(column, table.getConstraint()) == null);
    }

    private static OpenApiTableHandle withCursor(OpenApiTableHandle table, OpenApiColumn cursorColumn, String cursor)
    {
        TupleDomain<ColumnHandle> cursorConstraint = TupleDomain.fromFixedValues(Map.of(
                cursorColumn.getHandle(), NullableValue.of(cursorColumn.getType(), toNativeCursor(cursorColumn, cursor))));
        return table.cloneWithConstraint(table.getConstraint().isNone() ? cursorConstraint : table.getConstraint().intersect(cursorConstraint));
    }

    private static Object toNativeCursor(OpenApiColumn cursorColumn, String cursor)
    {
        if (cursorColumn.getType() instanceof VarcharType) {
            return utf8Slice(cursor);
        }
        Long value = Longs.tryParse(cursor);
        if (value == null) {
            // the cursor is not included, because it might contain sensitive information
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("The next cursor is not a whole number, but parameter %s is an integer", cursorColumn.getSourceName()));
        }
        return value;
    }

    private static OpenApiTableHandle withLimit(OpenApiTableHandle table, OpenApiColumn limitColumn, long limit)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>(table.getConstraint().getDomains().orElse(Map.of()));
        domains.put(limitColumn.getHandle(), Domain.singleValue(limitColumn.getType(), limit));
        return table.cloneWithConstraint(TupleDomain.withColumnDomains(domains));
    }

    private ResultsPage getPage(UserCredentials credentials, OpenApiTableHandle pageTable, HttpPath httpPath, Map<OpenApiColumn, Object> pathParameters, boolean isFirstPage, boolean revalidateCachedResponses)
    {
        // pages after the first are read only after the first one was found, so a missing one is an error, not the end of the results
        boolean notFoundIsEmpty = isFirstPage && isNotFoundEmpty(httpPath, pathParameters);
        // the body must be serialized for every page, since it can contain the page number,
        // and the response handler copies the page number to every row
        return makeRequest(credentials, pageTable, httpPath, pathParameters, getParameterValues(pageTable, httpPath), revalidateCachedResponses, getBodyGenerator(pageTable, httpPath), getResponseHandler(pageTable, httpPath, notFoundIsEmpty));
    }

    private ResultsPage getNextPage(
            UserCredentials credentials,
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            Optional<OpenApiColumn> cursorColumn,
            Optional<String> next,
            boolean revalidateCachedResponses)
    {
        if (next.isEmpty()) {
            return getPage(credentials, table, httpPath, pathParameters, true, revalidateCachedResponses);
        }
        if (cursorColumn.isPresent()) {
            return getPage(credentials, withCursor(table, cursorColumn.get(), next.get()), httpPath, pathParameters, false, revalidateCachedResponses);
        }
        // the link already has all query parameters, and the response handler checked that it points to the host of the base URI
        return sendRequest(credentials, table, httpPath, URI.create(next.get()), getParameterValues(table, httpPath), revalidateCachedResponses, getBodyGenerator(table, httpPath), getResponseHandler(table, httpPath, false));
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

    private JsonResponseHandler getResponseHandler(OpenApiTableHandle table, HttpPath httpPath, boolean notFoundIsEmpty)
    {
        return new JsonResponseHandler(
                table,
                httpPath,
                openApiSpec.getResultsPointers(table.getSchemaTableName()).get(httpPath),
                openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()),
                notFoundIsEmpty);
    }

    private boolean isNotFoundEmpty(HttpPath httpPath, Map<OpenApiColumn, Object> pathParameters)
    {
        // a single resource that doesn't exist has no rows, but a list that doesn't exist usually means that the base URI or the path is wrong
        return !pathParameters.isEmpty() || openApiSpec.declaresResponse(httpPath, String.valueOf(HttpStatus.NOT_FOUND.code()));
    }

    private BodyGenerator getBodyGenerator(OpenApiTableHandle table, HttpPath httpPath)
    {
        if (!hasRequestBody(httpPath.method())) {
            return null;
        }
        return toBodyGenerator(httpPath, serializeMap(table, httpPath));
    }

    private static boolean hasRequestBody(PathItem.HttpMethod method)
    {
        return method == PathItem.HttpMethod.POST || method == PathItem.HttpMethod.PUT || method == PathItem.HttpMethod.PATCH;
    }

    public void insertRows(ConnectorSession session, OpenApiOutputTableHandle table, Page page, int position)
    {
        UserCredentials credentials = extraCredentialProvider.getUserCredentials(session.getIdentity());
        writeRows(credentials, table, "INSERT", table.getTableHandle().getInsertPaths(), page, position, column -> getRowValue(table, page, position, column), Optional.empty());
    }

    public void updateRows(ConnectorSession session, OpenApiOutputTableHandle table, Page page, Block rowIds, int position, Set<OpenApiColumnHandle> updatedColumns)
    {
        UserCredentials credentials = extraCredentialProvider.getUserCredentials(session.getIdentity());
        List<HttpPath> paths = table.getTableHandle().getUpdatePaths();
        table.getTableHandle().checkHasPaths("UPDATE", paths);
        Function<OpenApiColumn, Object> rowValues = getIdentifiedRowValues(table.getTableHandle(), "UPDATE", paths, rowIds, position, column -> getRowValue(table, page, position, column));
        writeRows(credentials, table, "UPDATE", paths, page, position, rowValues, Optional.of(updatedColumns));
    }

    public void deleteRows(ConnectorSession session, OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        UserCredentials credentials = extraCredentialProvider.getUserCredentials(session.getIdentity());
        OpenApiTableHandle tableHandle = table.getTableHandle();
        tableHandle.checkHasPaths("DELETE", tableHandle.getDeletePaths());
        Function<OpenApiColumn, Object> rowValues = getIdentifiedRowValues(tableHandle, "DELETE", tableHandle.getDeletePaths(), rowIds, position, column -> getFilter(column, tableHandle.getConstraint()));
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(tableHandle, tableHandle.getDeletePaths(), rowValues);
        HttpPath httpPath = pathWithParams.getKey();
        makeRequest(
                credentials,
                tableHandle,
                httpPath,
                pathWithParams.getValue(),
                // operations marked for DELETE, like a POST, can require filters in the request body
                getBodyGenerator(tableHandle, httpPath),
                new AnyResponseHandler());
    }

    /**
     * Returns the values of path parameters identifying the resource of a row, decoded from its row ID, and other values from the given function.
     */
    private Function<OpenApiColumn, Object> getIdentifiedRowValues(
            OpenApiTableHandle table,
            String statement,
            List<HttpPath> paths,
            Block rowIds,
            int position,
            Function<OpenApiColumn, Object> otherValues)
    {
        Map<OpenApiColumn, JsonNode> identifiers = decodeRowId(table, rowIds, position);
        checkIdentified(table, statement, paths, identifiers);
        Set<OpenApiColumn> rowIdColumns = ImmutableSet.copyOf(getRowIdColumns(table));
        return column -> {
            // a filter can match many resources, so it doesn't identify the resource of a row
            if (rowIdColumns.contains(column)) {
                return identifiers.get(column);
            }
            return otherValues.apply(column);
        };
    }

    private Map<OpenApiColumn, JsonNode> decodeRowId(OpenApiTableHandle table, Block rowIds, int position)
    {
        checkState(!rowIds.isNull(position), "Row ID is null");
        JsonNode rowId;
        try {
            rowId = OBJECT_MAPPER.readTree(VARCHAR.getSlice(rowIds, position).toStringUtf8());
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
        return getRowIdColumns(table).stream()
                .filter(column -> rowId.hasNonNull(column.getName()))
                .collect(toImmutableMap(identity(), column -> rowId.get(column.getName())));
    }

    private void checkIdentified(OpenApiTableHandle table, String statement, List<HttpPath> paths, Map<OpenApiColumn, JsonNode> identifiers)
    {
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        Map<HttpPath, List<OpenApiColumn>> missingIdentifiers = paths.stream()
                .collect(toImmutableMap(identity(), httpPath -> columns.stream()
                        .filter(column -> isPredicate(column, httpPath, ParameterLocation.PATH) && !identifiers.containsKey(column))
                        .toList()));
        if (missingIdentifiers.values().stream().anyMatch(List::isEmpty)) {
            return;
        }
        // a request without them would change another resource than the one of the row, or none
        throw new TrinoException(INVALID_ROW_FILTER, format(
                "Cannot %s rows of table %s, because they don't have values of the path parameters that identify their resources: %s. " +
                        "Filter on the columns of these parameters, or link the parameters to the response fields with the same values, " +
                        "using the field property of the x-trino extension of the parameters",
                statement,
                table.getSchemaTableName().getTableName(),
                missingIdentifiers.entrySet().stream()
                        .map(entry -> entry.getValue().stream()
                                .map(column -> format("%s (column %s)", column.getParameterName(entry.getKey()), column.getName()))
                                .collect(joining(", ")) + " of " + entry.getKey())
                        .collect(joining("; "))));
    }

    /**
     * Columns of the path parameters of operations used by UPDATE and DELETE, which identify the resource of a row.
     */
    private List<OpenApiColumn> getRowIdColumns(OpenApiTableHandle table)
    {
        List<HttpPath> writePaths = Stream.concat(table.getUpdatePaths().stream(), table.getDeletePaths().stream()).toList();
        return openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(column -> writePaths.stream().anyMatch(httpPath -> isPredicate(column, httpPath, ParameterLocation.PATH)))
                .toList();
    }

    private void writeRows(
            UserCredentials credentials,
            OpenApiOutputTableHandle table,
            String statement,
            List<HttpPath> paths,
            Page page,
            int position,
            Function<OpenApiColumn, Object> rowValues,
            Optional<Set<OpenApiColumnHandle>> updatedColumns)
    {
        table.getTableHandle().checkHasPaths(statement, paths);
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
        makeRequest(
                credentials,
                table.getTableHandle(),
                httpPath,
                pathWithParams.getValue(),
                parameterValues,
                false,
                toBodyGenerator(httpPath, body),
                new AnyResponseHandler());
    }

    private BodyGenerator toBodyGenerator(HttpPath httpPath, ObjectNode fields)
    {
        // the spec can still be used for other operations, so an operation with only a multipart body fails when it's used
        Optional<String> multipartMediaType = openApiSpec.getMultipartRequestMediaType(httpPath);
        if (multipartMediaType.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, format("Operation %s only accepts %s request bodies, which are not supported", httpPath, multipartMediaType.get()));
        }
        if (openApiSpec.hasFormRequestBody(httpPath)) {
            return toFormBody(httpPath, fields);
        }
        try {
            return createStaticBodyGenerator(toBytes(toRequestBody(httpPath, fields)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BodyGenerator toFormBody(HttpPath httpPath, ObjectNode fields)
    {
        FormDataBodyBuilder body = new FormDataBodyBuilder();
        for (Map.Entry<String, JsonNode> field : fields.properties()) {
            // arrays are sent as repeated fields, like with the default form style, and forms can't send NULL values, so they're omitted
            List<JsonNode> values = field.getValue().isArray() ? ImmutableList.copyOf(field.getValue()) : List.of(field.getValue());
            values.stream()
                    .filter(value -> !value.isNull())
                    .forEach(value -> body.addField(field.getKey(), formatFormValue(httpPath, field.getKey(), value)));
        }
        StaticBodyGenerator bodyGenerator = body.build();
        logRequestBody(bodyGenerator.getBody());
        return bodyGenerator;
    }

    private static String formatFormValue(HttpPath httpPath, String name, JsonNode value)
    {
        if (!value.isValueNode()) {
            throw new TrinoException(NOT_SUPPORTED, format("Field %s of the form request body of %s has a nested value, which can't be sent as a form field", name, httpPath));
        }
        if (value.isNumber()) {
            return formatParamValue(value.numberValue());
        }
        return value.asText();
    }

    private JsonNode toRequestBody(HttpPath httpPath, ObjectNode fields)
    {
        // columns of array request bodies are the properties of its items, so every row is sent as a single item
        if (!openApiSpec.hasArrayRequestBody(httpPath)) {
            return fields;
        }
        return OBJECT_MAPPER.createArrayNode().add(fields);
    }

    public <T> T makeRequest(
            UserCredentials credentials,
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        return makeRequest(credentials, table, httpPath, pathParameters, getParameterValues(table, httpPath), false, bodyGenerator, responseHandler);
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
            UserCredentials credentials,
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
            String parameterName = column.getParameterName(httpPath);
            String value = getParameterStyle(column, httpPath, ParameterLocation.PATH).serializePathValue(parameterName, toJsonNode(column, entry.getValue()));
            if (value.isEmpty()) {
                throw new TrinoException(INVALID_ROW_FILTER, format("Path parameter %s can't be empty", column.getName()));
            }
            uriPath = uriPath.replace(format("{%s}", parameterName), value);
        }
        URI uri;
        try {
            uri = buildUri(servers.getUri(httpPath), uriPath, getQueryParams(table, httpPath, valueProvider));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        return sendRequest(credentials, table, httpPath, uri, valueProvider, revalidateCachedResponses, bodyGenerator, responseHandler);
    }

    private <T> T sendRequest(
            UserCredentials credentials,
            OpenApiTableHandle table,
            HttpPath httpPath,
            URI uri,
            Function<OpenApiColumn, Object> valueProvider,
            boolean revalidateCachedResponses,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        Request.Builder builder = new Request.Builder()
                .setMethod(httpPath.method().name())
                .setBodyGenerator(bodyGenerator)
                .setUri(uri)
                // the HTTP client would send custom headers, like API keys, to any host a server redirects to
                .setFollowRedirects(false)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(ACCEPT, openApiSpec.getResponseMediaType(httpPath).toString())
                .addHeader(OPENAPI_PATH_HEADER, httpPath.path());
        // some gateways and firewalls reject requests without a body that declare its type
        if (bodyGenerator != null) {
            builder.addHeader(CONTENT_TYPE, openApiSpec.getRequestMediaType(httpPath).toString());
        }
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
                isRead(table, httpPath),
                () -> {
                    // retries are requests too, and servers count them against their rate limits
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    return getHttpClient(credentials).execute(request, responseHandler);
                },
                (failure, attempts, notRetriedReason) -> toTrinoException(table, httpPath, request, failure, attempts, notRetriedReason));
    }

    private HttpClient getHttpClient(UserCredentials credentials)
    {
        HttpClient sizeLimitingClient = new SizeLimitingHttpClient(new AuthenticatingHttpClient(httpClient, authentication, credentials), maxResponseSize);
        // reading the body of a 401 response without a challenge fails below the size limit, but Jetty already limits the body it buffers in the failure to 16 KiB
        HttpClient unchallengedHttpClient = new UnchallengedResponseHttpClient(sizeLimitingClient);
        // the cache is wrapped by decompression, so it stores compressed responses
        return new DecompressingHttpClient(
                httpCache
                        .<HttpClient>map(cache -> new CachingHttpClient(unchallengedHttpClient, getHttpCachePartition(cache, credentials), Clock.systemUTC()))
                        .orElse(unchallengedHttpClient),
                maxResponseSize);
    }

    // the catalog credentials already name the cache directory, and responses got with credentials of a user are stored separately,
    // in a partition named like the directory, with a slow hash, because file names can be visible to other users of the server
    private HttpCache getHttpCachePartition(HttpCache cache, UserCredentials credentials)
    {
        if (credentials.isEmpty()) {
            return cache;
        }
        String secrets = credentials.toSecretString();
        HashCode key = Hashing.sha256().hashString(secrets, UTF_8);
        return cache.partition(uncheckedCacheGet(httpCachePartitions, key, () -> HashCode.fromBytes(hashSecrets(secrets, serverUris)).toString()));
    }

    // an operation that is used only by SELECT doesn't change anything, so repeating it is safe, even if it uses POST
    private static boolean isRead(OpenApiTableHandle table, HttpPath httpPath)
    {
        return table.getSelectPaths().contains(httpPath)
                && !table.getInsertPaths().contains(httpPath)
                && !table.getUpdatePaths().contains(httpPath)
                && !table.getDeletePaths().contains(httpPath);
    }

    private TrinoException toTrinoException(OpenApiTableHandle table, HttpPath httpPath, Request request, HttpRequestFailure failure, int attempts, Optional<String> notRetriedReason)
    {
        String context = describeFailedRequest(httpPath, request);
        String attemptsSuffix = attempts > 1 ? format(" after %d attempts", attempts) : "";
        return switch (failure) {
            case HttpRequestFailure.TransportError error -> {
                Optional<ResponseTooLargeException> tooLarge = getResponseTooLarge(error);
                if (tooLarge.isPresent()) {
                    yield new TrinoException(
                            OPENAPI_INVALID_RESPONSE,
                            format("%s%s: the response is larger than %s, increase the max-response-size catalog property to read it", context, attemptsSuffix, tooLarge.get().getMaxSize()),
                            error.getCause());
                }
                yield new TrinoException(
                        OPENAPI_CONNECTION_ERROR,
                        format("%s%s: %s", context, attemptsSuffix, requireNonNullElse(error.getCause().getMessage(), error.getCause().getClass().getName())),
                        error.getCause());
            }
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

    private static Optional<ResponseTooLargeException> getResponseTooLarge(HttpRequestFailure.TransportError error)
    {
        return Throwables.getCausalChain(error).stream()
                .filter(ResponseTooLargeException.class::isInstance)
                .map(ResponseTooLargeException.class::cast)
                .findFirst();
    }

    private String describeFailedRequest(HttpPath httpPath, Request request)
    {
        String operation = openApiSpec.getOperationId(httpPath)
                .map(operationId -> "operation " + operationId)
                .orElse("path " + httpPath.path());
        // the query string can contain filter values and API keys
        return format("Request %s %s for %s failed", request.getMethod(), withoutQuery(request.getUri()), operation);
    }

    static String describeRedirect(URI requestUri, HttpRequestFailure.ErrorResponse error)
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

    public static String withoutQuery(URI uri)
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
        // Double.toString() and Float.toString() use the scientific notation for large and small values, but not for NaN and infinity
        if ((value instanceof Double || value instanceof Float) && value.toString().contains("E")) {
            return new BigDecimal(value.toString()).stripTrailingZeros().toPlainString();
        }
        return value.toString();
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
        // reads make a request for every value, but writes make a single request for every row, which can only send one value
        if (!domain.isSingleValue()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Filter on %s has more than one value, but a request can only send one".formatted(column.getName()));
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

    private record ResultsPage(Iterable<List<?>> rows, OptionalLong totalResults, OptionalLong totalPages, Optional<Boolean> hasMore, Optional<String> nextCursor)
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
        private final boolean notFoundIsEmpty;

        JsonResponseHandler(OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, Pagination pagination, boolean notFoundIsEmpty)
        {
            this.table = requireNonNull(table, "table is null");
            this.httpPath = requireNonNull(httpPath, "httpPath is null");
            this.resultsPointer = resultsPointer;
            this.pagination = requireNonNull(pagination, "pagination is null");
            this.notFoundIsEmpty = notFoundIsEmpty;
        }

        @Override
        public ResultsPage handleException(Request request, Exception exception)
        {
            throw new HttpRequestFailure.TransportError(exception);
        }

        @Override
        public ResultsPage handle(Request request, Response response)
        {
            if (notFoundIsEmpty && response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                return new ResultsPage(List.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), Optional.empty());
            }
            // like writes, reads can respond with other success codes, like 201 Created to a search sent as a POST
            if (HttpStatus.familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                throw HttpRequestFailure.ErrorResponse.read(response);
            }
            if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                return new ResultsPage(List.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), Optional.empty());
            }
            JsonNode jsonNode = readJson(request, response);
            failOnReportedError(request, response.getStatusCode(), jsonNode);
            return new ResultsPage(
                    convertJson(request, table, httpPath, resultsPointer, jsonNode),
                    getLong(request, response, jsonNode, "totalResultsPath", pagination.totalResultsPath()),
                    getLong(request, response, jsonNode, "totalPagesPath", pagination.totalPagesPath()),
                    getBoolean(response, jsonNode, pagination.hasMorePath()),
                    getNextCursor(request, response, jsonNode));
        }

        private static JsonNode readJson(Request request, Response response)
        {
            try (CountingInputStream input = new CountingInputStream(response.getInputStream())) {
                // the parser reads the body directly, so it's not held in memory as a string too, unless it's logged
                JsonNode jsonNode = bodyLog.isDebugEnabled() ? readLoggedJson(response, input) : OBJECT_MAPPER.readTree(input);
                log.debug("Received response code %d with %d bytes", response.getStatusCode(), input.getCount());
                return jsonNode;
            }
            catch (JsonProcessingException ex) {
                String message = format("Response to %s is not valid JSON", describe(request));
                // the parser's message quotes the response, which might contain sensitive information, so it's only logged
                bodyLog.debug(ex, message);
                throw new TrinoException(OPENAPI_INVALID_RESPONSE, message);
            }
            catch (IOException e) {
                throw new HttpRequestFailure.TransportError(e);
            }
        }

        private static JsonNode readLoggedJson(Response response, InputStream input)
                throws IOException
        {
            String result = new InputStreamReader(input, UTF_8).readAllAsString();
            bodyLog.debug("Received response code %d: %s", response.getStatusCode(), result);
            return OBJECT_MAPPER.readTree(result);
        }

        // some servers respond with status 200 and an error instead of the results
        private void failOnReportedError(Request request, int statusCode, JsonNode jsonNode)
        {
            if (resultsPointer == null || JsonPointer.empty().equals(resultsPointer)) {
                return;
            }
            JsonNode resultNode = jsonNode.at(resultsPointer);
            if (resultNode.isArray() || resultNode.isObject()) {
                return;
            }
            JsonPointer errorPointer = openApiSpec.getErrorPointers(table.getSchemaTableName()).get(httpPath);
            if (errorPointer == null) {
                return;
            }
            JsonNode errorNode = jsonNode.at(errorPointer);
            if (errorNode.isMissingNode() || errorNode.isNull()) {
                return;
            }
            throw new TrinoException(OPENAPI_HTTP_ERROR, format("%s with HTTP %d: %s", describeFailedRequest(httpPath, request), statusCode, errorNode));
        }

        private Optional<String> getNextCursor(Request request, Response response, JsonNode jsonNode)
        {
            if (!pagination.nextPageLinkHeader()) {
                return getCursor(request, response, jsonNode, pagination.nextCursorPath());
            }
            Optional<URI> nextLink = getNextLink(request, response);
            if (nextLink.isEmpty()) {
                return Optional.empty();
            }
            if (pagination.cursorParam().isPresent()) {
                return getQueryParameter(nextLink.get(), pagination.cursorParam().get());
            }
            // requests have credentials, which must not be sent to any host the server links to
            if (!isSameOrigin(servers.getUri(httpPath), nextLink.get())) {
                throw new TrinoException(OPENAPI_INVALID_RESPONSE, format(
                        "Response to %s links to the next page on another host, %s, but requests are sent only to the server of the operation, so credentials are not sent to other hosts",
                        describe(request),
                        withoutQuery(nextLink.get())));
            }
            return Optional.of(nextLink.get().toString());
        }

        private static Optional<URI> getNextLink(Request request, Response response)
        {
            Optional<String> nextLink = LinkHeader.getNextLink(response.getHeaders().get(HeaderName.of(LINK)));
            if (nextLink.isEmpty()) {
                return Optional.empty();
            }
            try {
                return Optional.of(request.getUri().resolve(nextLink.get()));
            }
            catch (IllegalArgumentException _) {
                // the link is not included, because it might contain sensitive information
                throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response to %s has an invalid link to the next page in the Link header", describe(request)));
            }
        }

        private static Optional<String> getQueryParameter(URI uri, String name)
        {
            return Splitter.on('&').omitEmptyStrings().splitToStream(nullToEmpty(uri.getRawQuery()))
                    .map(parameter -> parameter.split("=", 2))
                    .filter(parameter -> URLDecoder.decode(parameter[0], UTF_8).equals(name))
                    .map(parameter -> parameter.length == 2 ? URLDecoder.decode(parameter[1], UTF_8) : "")
                    .filter(value -> !value.isEmpty())
                    .findFirst();
        }

        private static boolean isSameOrigin(URI uri, URI other)
        {
            return uri.getScheme().equalsIgnoreCase(other.getScheme())
                    && uri.getHost().equalsIgnoreCase(nullToEmpty(other.getHost()))
                    && getPort(uri) == getPort(other);
        }

        private static int getPort(URI uri)
        {
            if (uri.getPort() != -1) {
                return uri.getPort();
            }
            return uri.getScheme().equalsIgnoreCase("https") ? 443 : 80;
        }

        private static JsonNode getValue(Response response, JsonNode jsonNode, ResponsePath path)
        {
            return switch (path) {
                case ResponsePath.Body(JsonPointer pointer) -> jsonNode.at(pointer);
                case ResponsePath.Header(String name) -> response.getHeaders().get(HeaderName.of(name)).stream()
                        .findFirst()
                        .<JsonNode>map(TextNode::valueOf)
                        .orElse(MissingNode.getInstance());
            };
        }

        private static OptionalLong getLong(Request request, Response response, JsonNode jsonNode, String propertyName, Optional<ResponsePath> path)
        {
            if (path.isEmpty()) {
                return OptionalLong.empty();
            }
            JsonNode valueNode = getValue(response, jsonNode, path.get());
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
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Value of %s %s in the response to %s is not a whole number", propertyName, path.get(), describe(request)));
        }

        private static Optional<String> getCursor(Request request, Response response, JsonNode jsonNode, Optional<ResponsePath> path)
        {
            if (path.isEmpty()) {
                return Optional.empty();
            }
            JsonNode valueNode = getValue(response, jsonNode, path.get());
            if (valueNode.isMissingNode() || valueNode.isNull()) {
                return Optional.empty();
            }
            if (!valueNode.isTextual() && !valueNode.isIntegralNumber()) {
                throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Value of nextCursorPath %s in the response to %s is a JSON %s, not a string or a whole number", path.get(), describe(request), valueNode.getNodeType()));
            }
            // servers can send an empty cursor with the last page, instead of leaving it out
            return Optional.of(valueNode.asText()).filter(cursor -> !cursor.isEmpty());
        }

        private static Optional<Boolean> getBoolean(Response response, JsonNode jsonNode, Optional<ResponsePath> path)
        {
            if (path.isEmpty()) {
                return Optional.empty();
            }
            JsonNode valueNode = getValue(response, jsonNode, path.get());
            if (valueNode.isBoolean()) {
                return Optional.of(valueNode.booleanValue());
            }
            // header values are always text
            if (path.get() instanceof ResponsePath.Header && (valueNode.asText().equalsIgnoreCase("true") || valueNode.asText().equalsIgnoreCase("false"))) {
                return Optional.of(Boolean.parseBoolean(valueNode.asText()));
            }
            return Optional.empty();
        }
    }

    private Iterable<List<?>> convertJson(Request request, OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        Map<String, Object> recordParams = getRecordFilterValues(table, httpPath);
        Set<OpenApiColumn> rowIdColumns = ImmutableSet.copyOf(getRowIdColumns(table));

        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.addAll(convertJsonToRecords(request, table, httpPath, resultsPointer, rowIdColumns, recordParams, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.addAll(convertJsonToRecords(request, table, httpPath, resultsPointer, rowIdColumns, recordParams, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private Iterable<List<?>> convertJsonToRecords(
            Request request,
            OpenApiTableHandle table,
            HttpPath httpPath,
            JsonPointer resultsPointer,
            Set<OpenApiColumn> rowIdColumns,
            Map<String, Object> recordParams,
            JsonNode jsonNode)
    {
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        boolean hasResultsPointer = resultsPointer != null && !JsonPointer.empty().equals(resultsPointer);
        // without resultsPath, elements of a response that is an array of plain values are the results
        boolean resultsArePlainValues = !hasResultsPointer && columns.stream().anyMatch(column -> column.getPlainValuePaths().contains(httpPath));
        if (!jsonNode.isObject() && !resultsArePlainValues) {
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response to %s contains a JSON %s where an object was expected", describe(request), jsonNode.getNodeType()));
        }

        Iterable<JsonNode> resultNodes = List.of(jsonNode);
        if (hasResultsPointer) {
            resultNodes = getResultNodes(request, resultsPointer, jsonNode.at(resultsPointer));
        }

        boolean resultsAreMaps = columns.stream().anyMatch(column -> column.getEntryKeyPaths().contains(httpPath));
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (JsonNode resultNode : resultNodes) {
            if (!resultsAreMaps) {
                resultRecordsBuilder.add(convertToRecord(request, httpPath, columns, rowIdColumns, recordParams, jsonNode, resultNode, Optional.empty()));
                continue;
            }
            for (Map.Entry<String, JsonNode> entry : getEntries(request, resultNode)) {
                resultRecordsBuilder.add(convertToRecord(request, httpPath, columns, rowIdColumns, recordParams, jsonNode, entry.getValue(), Optional.of(entry.getKey())));
            }
        }

        return resultRecordsBuilder.build();
    }

    private List<?> convertToRecord(
            Request request,
            HttpPath httpPath,
            List<OpenApiColumn> columns,
            Set<OpenApiColumn> rowIdColumns,
            Map<String, Object> recordParams,
            JsonNode jsonNode,
            JsonNode resultNode,
            Optional<String> entryKey)
    {
        List<Object> recordBuilder = new ArrayList<>();
        // values are kept the way they're sent in requests, and names of columns are unique, unlike names of parameters of different operations
        ObjectNode rowId = OBJECT_MAPPER.createObjectNode();
        int rowIdField = -1;
        for (OpenApiColumn column : columns) {
            if (column.getName().equals(ROW_ID)) {
                // the row ID holds values of other columns, which are read later
                rowIdField = recordBuilder.size();
                recordBuilder.add(null);
                continue;
            }
            if (column.getEntryKeyPaths().contains(httpPath)) {
                recordBuilder.add(entryKey.orElseThrow());
                continue;
            }
            if (column.getPlainValuePaths().contains(httpPath)) {
                recordBuilder.add(convertToRecordValue(request, column, resultNode));
                continue;
            }
            String parameterName = column.getSourceName();
            JsonNode sourceNode = column.getEnvelopePaths().contains(httpPath) ? jsonNode : resultNode;
            // request parameters are usually not included in the response, so copy them from predicates
            if (isPredicate(column, httpPath, null) && (column.isRequestOnly() || !sourceNode.has(parameterName))) {
                Object value = recordParams.getOrDefault(parameterName, null);
                recordBuilder.add(value);
                if (value != null && rowIdColumns.contains(column)) {
                    rowId.set(column.getName(), toJsonNode(column, toParameterValue(column, value)));
                }
                continue;
            }
            JsonNode value = sourceNode.get(parameterName);
            recordBuilder.add(convertToRecordValue(request, column, value));
            // rows of a list are different resources, so they're identified by the values in the response, not by the filters
            if (value != null && !value.isNull() && rowIdColumns.contains(column)) {
                rowId.set(column.getName(), value);
            }
        }
        if (rowIdField != -1) {
            recordBuilder.set(rowIdField, rowId.toString());
        }
        return recordBuilder;
    }

    private static Set<Map.Entry<String, JsonNode>> getEntries(Request request, JsonNode resultNode)
    {
        // like a null element of an array of objects with dynamic keys
        if (resultNode.isNull()) {
            return Set.of();
        }
        if (!resultNode.isObject()) {
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response to %s contains a JSON %s where an object was expected", describe(request), resultNode.getNodeType()));
        }
        return resultNode.properties();
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

    private static Iterable<JsonNode> getResultNodes(Request request, JsonPointer resultsPointer, JsonNode resultNode)
    {
        if (resultNode instanceof ArrayNode) {
            return resultNode;
        }
        // results can be a union of a single object and an array of objects
        if (resultNode.isObject()) {
            return List.of(resultNode);
        }
        // some servers send null instead of an empty list
        if (resultNode.isNull()) {
            return List.of();
        }
        // the response is not included, because it might contain sensitive information, and it's in the body log
        if (resultNode.isMissingNode()) {
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response to %s has no value at resultsPath %s", describe(request), resultsPointer));
        }
        throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Value of resultsPath %s in the response to %s is a JSON %s, not an array or an object", resultsPointer, describe(request), resultNode.getNodeType()));
    }

    private byte[] toBytes(JsonNode rootNode)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OBJECT_MAPPER.createGenerator(baos).writeTree(rootNode);
        byte[] output = baos.toByteArray();
        logRequestBody(output);
        return output;
    }

    private static void logRequestBody(byte[] body)
    {
        log.debug("Request body of %d bytes", body.length);
        if (bodyLog.isDebugEnabled()) {
            bodyLog.debug("Request body: %s", new String(body, UTF_8));
        }
    }

    private static class AnyResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        private boolean sent;

        @Override
        public Void handleException(Request request, Exception exception)
        {
            sent = true;
            throw new HttpRequestFailure.TransportError(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            boolean retried = sent;
            sent = true;
            // the previous attempt could have deleted the resource before failing with a gateway error or a timeout
            if (retried && request.getMethod().equals("DELETE") && isGone(response.getStatusCode())) {
                log.debug("Received response code %d to a retried DELETE, so the resource was already deleted", response.getStatusCode());
                return null;
            }
            // servers often respond to writes with 201 Created, 202 Accepted, or 204 No Content
            if (HttpStatus.familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                throw HttpRequestFailure.ErrorResponse.read(response);
            }
            log.debug("Received response code %d", response.getStatusCode());
            // the body of a response to a write is not used, so it's read only to be logged
            if (bodyLog.isDebugEnabled()) {
                bodyLog.debug("Received response code %d: %s", response.getStatusCode(), readBody(response));
            }
            return null;
        }

        private static String readBody(Response response)
        {
            try (InputStream input = response.getInputStream()) {
                return new InputStreamReader(input, UTF_8).readAllAsString();
            }
            catch (IOException e) {
                throw new HttpRequestFailure.TransportError(e);
            }
        }

        private static boolean isGone(int statusCode)
        {
            return statusCode == HttpStatus.NOT_FOUND.code() || statusCode == HttpStatus.GONE.code();
        }
    }

    private interface PageGetter
    {
        // the number of results read before the page is its offset only when every page is read one after another, starting from the first one
        ResultsPage get(int page, long readResults);
    }

    private Iterable<List<?>> pageIterator(
            PageGetter getter,
            int offset,
            long rowLimit,
            long maxResults,
            int pageIncrement,
            OptionalLong pageSize,
            OptionalInt maxPages,
            Predicate<List<?>> rowFilter,
            Runnable acquireRequest)
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
                acquireRequest.run();
                ResultsPage resultsPage = getter.get(page, resultSize);
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

    private Iterable<List<?>> cursorIterator(
            Function<Optional<String>, ResultsPage> getter,
            long rowLimit,
            long maxResults,
            OptionalInt maxPages,
            Predicate<List<?>> rowFilter,
            Runnable acquireRequest)
    {
        return () -> new Iterator<>()
        {
            // results are counted before the row filter, because that's what the server counts
            long resultSize;
            long resultLimit = maxResults;
            long rowCount;
            int pageCount;
            Optional<String> cursor = Optional.empty();
            // servers can return the same cursor again with the last page, and requesting it again would never end pagination
            final Set<String> requestedCursors = new HashSet<>();
            boolean isLastPage;
            Iterator<List<?>> rows;

            @Override
            public boolean hasNext()
            {
                // empty pages don't end pagination, because servers can return them with a cursor to the next page,
                // like when they filter results after reading them from their storage
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
                acquireRequest.run();
                ResultsPage resultsPage = getter.apply(cursor);
                pageCount += 1;
                resultsPage.totalResults().ifPresent(totalResults -> resultLimit = min(resultLimit, totalResults));
                resultSize += Iterables.size(resultsPage.rows());
                cursor = resultsPage.nextCursor();
                if (cursor.isEmpty() || !requestedCursors.add(cursor.get())) {
                    isLastPage = true;
                }
                if (resultsPage.hasMore().isPresent() && !resultsPage.hasMore().get()) {
                    isLastPage = true;
                }
                if (resultsPage.totalPages().isPresent() && pageCount >= resultsPage.totalPages().orElseThrow()) {
                    isLastPage = true;
                }
                if (maxPages.isPresent() && pageCount >= maxPages.orElseThrow()) {
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

    // responses depend on the server and the account, so a catalog created again with the same name, but other servers or another account,
    // can't reuse responses of the previous one; passwords and client secrets are left out, because the username or client ID already identify the account
    private static Path getHttpCacheDirectory(CatalogCredentials credentials, OpenApiServers servers, CatalogName catalogName, String directory)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        String serverUris = getServerUris(servers);
        Stream.concat(
                        Stream.of(
                                serverUris,
                                credentials.getAuthenticationType().name(),
                                credentials.getAuthenticationScheme().name(),
                                credentials.getUsername().orElse(""),
                                credentials.getClientId().orElse(""),
                                credentials.getApiKeyName().orElse("")),
                        new TreeMap<>(credentials.getApiKeys()).keySet().stream())
                .forEach(value -> hasher.putString(value, UTF_8).putByte((byte) 0));
        hasher.putBytes(hashTokens(credentials, serverUris));
        return Path.of(directory, catalogName + "-" + hasher.hash());
    }

    private static String getServerUris(OpenApiServers servers)
    {
        return servers.getUris().stream()
                .map(URI::toString)
                .sorted()
                .collect(joining("\0"));
    }

    // bearer tokens and API key values are the only identity of their accounts, and the directory name is visible to other users,
    // so they're hashed with a slow function, which makes guessing them from the name impractical
    private static byte[] hashTokens(CatalogCredentials credentials, String serverUris)
    {
        String tokens = Stream.concat(
                        Stream.of(credentials.getBearerToken().orElse(""), credentials.getApiKeyValue().orElse("")),
                        new TreeMap<>(credentials.getApiKeys()).values().stream())
                .collect(joining("\0"));
        if (CharMatcher.is('\0').matchesAllOf(tokens)) {
            return new byte[0];
        }
        return hashSecrets(tokens, serverUris);
    }

    private static byte[] hashSecrets(String secrets, String salt)
    {
        PBEKeySpec keySpec = new PBEKeySpec(secrets.toCharArray(), salt.getBytes(UTF_8), TOKEN_HASH_ITERATIONS, 256);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(keySpec).getEncoded();
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to hash the credentials for the HTTP cache", e);
        }
        finally {
            keySpec.clearPassword();
        }
    }
}
