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

package pl.net.was.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.HeaderNames.AGE;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.DATE;
import static io.airlift.http.client.HeaderNames.ETAG;
import static io.airlift.http.client.HeaderNames.EXPIRES;
import static io.airlift.http.client.HeaderNames.IF_MODIFIED_SINCE;
import static io.airlift.http.client.HeaderNames.IF_NONE_MATCH;
import static io.airlift.http.client.HeaderNames.LAST_MODIFIED;
import static io.airlift.http.client.HeaderNames.VARY;
import static java.lang.Math.max;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNull;

// a stored response, with the times needed to calculate its age (RFC 9111, section 4.2.3),
// and the values of the request headers listed in its Vary header (RFC 9111, section 4.1).
// Stored responses are never compared, so comparing the body by reference in equals() doesn't matter
@SuppressWarnings("ArrayRecordComponent")
public record CachedResponse(
        int statusCode,
        List<Header> headers,
        Map<String, String> varyingRequestHeaders,
        long requestTimeMillis,
        long responseTimeMillis,
        byte[] body)
{
    // headers that describe the stored body, so they're not updated from a 304 response, see RFC 9111, section 3.2
    private static final Set<String> BODY_HEADERS = Set.of("content-length", "content-encoding", "transfer-encoding", "content-range");
    // they belong to the exchange they were sent in, and can carry session tokens, so they're neither stored, nor replayed with a stored response
    private static final Set<String> UNSTORED_HEADERS = Set.of("set-cookie", "set-cookie2", "authentication-info", "proxy-authentication-info");

    public CachedResponse
    {
        headers = ImmutableList.copyOf(requireNonNull(headers, "headers is null"));
        varyingRequestHeaders = Map.copyOf(requireNonNull(varyingRequestHeaders, "varyingRequestHeaders is null"));
        requireNonNull(body, "body is null");
    }

    public record Header(String name, String value)
    {
        public Header
        {
            requireNonNull(name, "name is null");
            requireNonNull(value, "value is null");
        }
    }

    static CachedResponse fromResponse(Request request, BufferedResponse response, long requestTimeMillis, long responseTimeMillis)
    {
        List<Header> headers = response.headers().entries().stream()
                .filter(entry -> !UNSTORED_HEADERS.contains(entry.getKey().toString().toLowerCase(Locale.ENGLISH)))
                .map(entry -> new Header(entry.getKey().toString(), entry.getValue()))
                .collect(toImmutableList());
        return new CachedResponse(
                response.statusCode(),
                headers,
                varyingRequestHeaders(request, headers),
                requestTimeMillis,
                responseTimeMillis,
                response.body());
    }

    // see RFC 9111, section 4.3.4
    CachedResponse revalidated(ListMultimap<HeaderName, String> notModifiedHeaders, long requestTimeMillis, long responseTimeMillis)
    {
        Set<String> updatedNames = notModifiedHeaders.keySet().stream()
                .map(name -> name.toString().toLowerCase(Locale.ENGLISH))
                .filter(name -> !BODY_HEADERS.contains(name) && !UNSTORED_HEADERS.contains(name))
                .collect(toImmutableSet());
        ImmutableList.Builder<Header> headers = ImmutableList.builder();
        this.headers.stream()
                .filter(header -> !updatedNames.contains(header.name().toLowerCase(Locale.ENGLISH)))
                .forEach(headers::add);
        notModifiedHeaders.entries().stream()
                .filter(entry -> updatedNames.contains(entry.getKey().toString().toLowerCase(Locale.ENGLISH)))
                .forEach(entry -> headers.add(new Header(entry.getKey().toString(), entry.getValue())));
        return new CachedResponse(statusCode, headers.build(), varyingRequestHeaders, requestTimeMillis, responseTimeMillis, body);
    }

    boolean matches(Request request)
    {
        return varyingRequestHeaders.entrySet().stream()
                .allMatch(entry -> entry.getValue().equals(requestHeaderValue(request, entry.getKey())));
    }

    boolean isFresh(long nowMillis)
    {
        if (cacheControl().noCache()) {
            return false;
        }
        return freshnessLifetimeMillis() > currentAgeMillis(nowMillis);
    }

    boolean hasValidators()
    {
        return header(ETAG).isPresent() || header(LAST_MODIFIED).isPresent();
    }

    Request conditionalRequest(Request request)
    {
        Request.Builder builder = Request.Builder.fromRequest(request);
        header(ETAG).ifPresent(etag -> builder.setHeader(IF_NONE_MATCH, etag));
        header(LAST_MODIFIED).ifPresent(lastModified -> builder.setHeader(IF_MODIFIED_SINCE, lastModified));
        return builder.build();
    }

    Response toResponse(long nowMillis)
    {
        ImmutableListMultimap.Builder<HeaderName, String> responseHeaders = ImmutableListMultimap.builder();
        headers.stream()
                .filter(header -> !header.name().equalsIgnoreCase(AGE.toString()))
                .forEach(header -> responseHeaders.put(HeaderName.of(header.name()), header.value()));
        responseHeaders.put(AGE, String.valueOf(currentAgeMillis(nowMillis) / 1000));
        return new BufferedResponse(statusCode, responseHeaders.build(), body);
    }

    CacheControl cacheControl()
    {
        return CacheControl.parse(headers(CACHE_CONTROL));
    }

    private long freshnessLifetimeMillis()
    {
        OptionalLong maxAge = cacheControl().maxAge();
        if (maxAge.isPresent()) {
            return maxAge.orElseThrow() * 1000;
        }
        Optional<Instant> expires = header(EXPIRES).flatMap(CachedResponse::parseDate);
        if (expires.isEmpty()) {
            // an Expires header with an invalid date, like 0, means the response is already expired
            return 0;
        }
        Instant date = header(DATE).flatMap(CachedResponse::parseDate).orElse(Instant.ofEpochMilli(responseTimeMillis));
        return expires.get().toEpochMilli() - date.toEpochMilli();
    }

    private long currentAgeMillis(long nowMillis)
    {
        long dateMillis = header(DATE).flatMap(CachedResponse::parseDate).map(Instant::toEpochMilli).orElse(responseTimeMillis);
        long apparentAge = max(0, responseTimeMillis - dateMillis);
        long ageValue = header(AGE).map(CachedResponse::parseSeconds).orElse(0L) * 1000;
        long correctedAgeValue = ageValue + (responseTimeMillis - requestTimeMillis);
        long correctedInitialAge = max(apparentAge, correctedAgeValue);
        return correctedInitialAge + (nowMillis - responseTimeMillis);
    }

    private Optional<String> header(HeaderName name)
    {
        return headers(name).stream().findFirst();
    }

    private List<String> headers(HeaderName name)
    {
        return headers.stream()
                .filter(header -> header.name().equalsIgnoreCase(name.toString()))
                .map(Header::value)
                .collect(toImmutableList());
    }

    private static Map<String, String> varyingRequestHeaders(Request request, List<Header> headers)
    {
        return headers.stream()
                .filter(header -> header.name().equalsIgnoreCase(VARY.toString()))
                .flatMap(header -> Arrays.stream(header.value().split(",")))
                .map(name -> name.trim().toLowerCase(Locale.ENGLISH))
                .filter(name -> !name.isEmpty())
                .distinct()
                .collect(toImmutableMap(name -> name, name -> requestHeaderValue(request, name)));
    }

    private static String requestHeaderValue(Request request, String name)
    {
        return String.join(",", request.getHeaders().get(HeaderName.of(name)));
    }

    private static Optional<Instant> parseDate(String value)
    {
        try {
            return Optional.of(ZonedDateTime.parse(value, RFC_1123_DATE_TIME).toInstant());
        }
        catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private static long parseSeconds(String value)
    {
        try {
            return Long.parseLong(value.trim());
        }
        catch (NumberFormatException e) {
            return 0;
        }
    }
}
