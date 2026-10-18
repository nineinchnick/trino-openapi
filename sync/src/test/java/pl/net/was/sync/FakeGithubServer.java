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

package pl.net.was.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Serves GitHub-shaped responses for a few endpoints, supporting the query parameters the sync steps use,
 * and records every request, so tests can check what a sync run asked for.
 */
public class FakeGithubServer
        implements Closeable
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_PAGE_SIZE = 30;

    private final HttpServer server;
    private final Map<String, Resource> resources = new ConcurrentHashMap<>();
    private final List<URI> requests = new CopyOnWriteArrayList<>();

    public FakeGithubServer()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    public String getUrl()
    {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Serves items as a JSON array, or, when {@code wrapperField} is set, as an object with a total count,
     * like the GitHub Actions endpoints do.
     */
    public void putList(String path, Optional<String> wrapperField, Optional<String> sinceField, List<Map<String, Object>> items)
    {
        resources.put(path, new Resource(false, wrapperField, sinceField, new CopyOnWriteArrayList<>(items)));
    }

    public void putList(String path, List<Map<String, Object>> items)
    {
        putList(path, Optional.empty(), Optional.empty(), items);
    }

    public void putObject(String path, Map<String, Object> item)
    {
        resources.put(path, new Resource(true, Optional.empty(), Optional.empty(), new CopyOnWriteArrayList<>(List.of(item))));
    }

    /**
     * Replaces the item with the same value of {@code keyField}, or adds it at the start, where GitHub lists the newest items.
     */
    public void upsert(String path, String keyField, Map<String, Object> item)
    {
        List<Map<String, Object>> items = requireNonNull(resources.get(path), "No resource at " + path).items();
        Object key = requireNonNull(item.get(keyField), "item has no " + keyField);
        for (int i = 0; i < items.size(); i++) {
            if (key.equals(items.get(i).get(keyField))) {
                items.set(i, item);
                return;
            }
        }
        items.addFirst(item);
    }

    public List<URI> getRequests()
    {
        return ImmutableList.copyOf(requests);
    }

    public List<URI> getRequests(String path)
    {
        return requests.stream()
                .filter(uri -> uri.getPath().equals(path))
                .collect(toImmutableList());
    }

    public void clearRequests()
    {
        requests.clear();
    }

    @Override
    public void close()
    {
        server.stop(0);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        URI uri = exchange.getRequestURI();
        requests.add(uri);
        Resource resource = resources.get(uri.getPath());
        if (resource == null) {
            respond(exchange, 404, ImmutableMap.of("message", "Not Found"));
            return;
        }
        if (resource.single()) {
            respond(exchange, 200, resource.items().getFirst());
            return;
        }
        Map<String, String> query = parseQuery(uri.getRawQuery());
        List<Map<String, Object>> matching = filter(resource, query);
        int pageSize = Integer.parseInt(query.getOrDefault("per_page", String.valueOf(DEFAULT_PAGE_SIZE)));
        int page = Integer.parseInt(query.getOrDefault("page", "1"));
        List<Map<String, Object>> pageItems = matching.stream()
                .skip((long) (page - 1) * pageSize)
                .limit(pageSize)
                .collect(toImmutableList());
        if (resource.wrapperField().isEmpty()) {
            respond(exchange, 200, pageItems);
            return;
        }
        respond(exchange, 200, ImmutableMap.of("total_count", matching.size(), resource.wrapperField().get(), pageItems));
    }

    private static List<Map<String, Object>> filter(Resource resource, Map<String, String> query)
    {
        Stream<Map<String, Object>> items = resource.items().stream();
        String state = query.get("state");
        if (state != null && !state.equals("all")) {
            items = items.filter(item -> state.equals(item.get("state")));
        }
        String status = query.get("status");
        if (status != null) {
            items = items.filter(item -> status.equals(item.get("status")));
        }
        String since = query.get("since");
        if (since != null) {
            String sinceField = resource.sinceField().orElse("updated_at");
            Instant sinceInstant = Instant.parse(since);
            items = items.filter(item -> !Instant.parse((String) getField(item, sinceField)).isBefore(sinceInstant));
        }
        String sort = query.get("sort");
        if (sort != null) {
            Comparator<Map<String, Object>> comparator = Comparator.comparing(item -> Instant.parse((String) item.get(sort + "_at")));
            items = items.sorted("asc".equals(query.get("direction")) ? comparator : comparator.reversed());
        }
        return items.collect(toImmutableList());
    }

    private static Object getField(Map<String, Object> item, String dottedName)
    {
        Object value = item;
        for (String name : Splitter.on('.').split(dottedName)) {
            value = ((Map<?, ?>) value).get(name);
        }
        return value;
    }

    private static Map<String, String> parseQuery(String rawQuery)
    {
        Map<String, String> query = new LinkedHashMap<>();
        if (rawQuery == null) {
            return query;
        }
        for (String pair : Splitter.on('&').split(rawQuery)) {
            List<String> parts = Splitter.on('=').limit(2).splitToList(pair);
            query.put(URLDecoder.decode(parts.get(0), UTF_8), parts.size() > 1 ? URLDecoder.decode(parts.get(1), UTF_8) : "");
        }
        return query;
    }

    private static void respond(HttpExchange exchange, int status, Object body)
            throws IOException
    {
        byte[] bytes = MAPPER.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private record Resource(boolean single, Optional<String> wrapperField, Optional<String> sinceField, List<Map<String, Object>> items) {}
}
