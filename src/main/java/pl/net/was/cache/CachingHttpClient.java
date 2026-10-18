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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;

import java.time.Clock;
import java.util.Optional;
import java.util.Set;

import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.ETAG;
import static io.airlift.http.client.HeaderNames.EXPIRES;
import static io.airlift.http.client.HeaderNames.LAST_MODIFIED;
import static io.airlift.http.client.HeaderNames.VARY;
import static java.util.Objects.requireNonNull;

// a private HTTP cache, following RFC 9111, for requests executed with the delegate client
public class CachingHttpClient
        implements HttpClient
{
    private static final Set<String> CACHEABLE_METHODS = Set.of("GET", "HEAD");
    private static final Set<Integer> CACHEABLE_STATUS_CODES = Set.of(200, 203, 404, 410);
    private static final int NOT_MODIFIED = 304;

    private final HttpClient delegate;
    private final HttpCache cache;
    private final Clock clock;

    public CachingHttpClient(HttpClient delegate, HttpCache cache, Clock clock)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.clock = requireNonNull(clock, "clock is null");
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        if (!CACHEABLE_METHODS.contains(request.getMethod())) {
            return executeUnsafe(request, responseHandler);
        }
        CacheControl requestCacheControl = CacheControl.parse(request.getHeaders().get(CACHE_CONTROL));
        if (requestCacheControl.noStore()) {
            return delegate.execute(request, responseHandler);
        }
        Optional<CachedResponse> stored = cache.get(request.getMethod(), request.getUri())
                .filter(response -> response.matches(request));
        if (stored.isPresent() && !requestCacheControl.noCache() && stored.get().isFresh(clock.millis())) {
            return responseHandler.handle(request, stored.get().toResponse(clock.millis()));
        }

        Request outgoingRequest = stored
                .filter(CachedResponse::hasValidators)
                .map(response -> response.conditionalRequest(request))
                .orElse(request);
        long requestTime = clock.millis();
        BufferedResponse response;
        try {
            response = delegate.execute(outgoingRequest, BufferedResponse.HANDLER);
        }
        catch (BufferedResponse.FailedRequestException e) {
            return responseHandler.handleException(request, e.getFailure());
        }
        long responseTime = clock.millis();

        if (response.statusCode() == NOT_MODIFIED && stored.isPresent()) {
            CachedResponse revalidated = stored.get().revalidated(response.headers(), requestTime, responseTime);
            cache.put(request.getMethod(), request.getUri(), revalidated);
            return responseHandler.handle(request, revalidated.toResponse(clock.millis()));
        }
        if (isStorable(response)) {
            cache.put(request.getMethod(), request.getUri(), CachedResponse.fromResponse(request, response, requestTime, responseTime));
        }
        else if (stored.isPresent()) {
            cache.remove(request.getMethod(), request.getUri());
        }
        return responseHandler.handle(request, response);
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return delegate.executeAsync(request, responseHandler);
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return delegate.executeStreaming(request);
    }

    @Override
    public RequestStats getStats()
    {
        return delegate.getStats();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isClosed()
    {
        return delegate.isClosed();
    }

    private <T, E extends Exception> T executeUnsafe(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return delegate.execute(request, new ResponseHandler<T, E>()
        {
            @Override
            public T handleException(Request request, Exception exception)
                    throws E
            {
                return responseHandler.handleException(request, exception);
            }

            @Override
            public T handle(Request request, Response response)
                    throws E
            {
                // a successful change makes stored responses of the same resource outdated, see RFC 9111, section 4.4
                if (response.getStatusCode() < 400) {
                    CACHEABLE_METHODS.forEach(method -> cache.remove(method, request.getUri()));
                }
                return responseHandler.handle(request, response);
            }
        });
    }

    private static boolean isStorable(BufferedResponse response)
    {
        if (!CACHEABLE_STATUS_CODES.contains(response.statusCode())) {
            return false;
        }
        if (CacheControl.parse(response.getHeaders(CACHE_CONTROL)).noStore()) {
            return false;
        }
        if (response.getHeaders(VARY).stream().anyMatch(value -> value.trim().equals("*"))) {
            return false;
        }
        // without heuristic freshness, a response is only useful when it's fresh for some time, or can be revalidated
        return CacheControl.parse(response.getHeaders(CACHE_CONTROL)).maxAge().isPresent()
                || response.getHeader(EXPIRES).isPresent()
                || response.getHeader(ETAG).isPresent()
                || response.getHeader(LAST_MODIFIED).isPresent();
    }
}
