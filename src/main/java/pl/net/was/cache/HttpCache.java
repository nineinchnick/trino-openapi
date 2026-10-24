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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.PRAGMA;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

// stores responses in files, so they survive restarts, and evicts the least recently used ones over the size limit;
// a file has a header with the format version and the length of the metadata, the metadata as JSON, and the raw body,
// so the body is written and read without being encoded or copied;
// a key is a hash of the method and URI, so all stored responses of a resource can be found, and a hash of the request headers,
// because header and cookie parameters filter the response, and servers rarely list them in the Vary header;
// partitions store responses sent with different credentials in separate entries, sharing the size limit and the invalidation of changed resources
public final class HttpCache
{
    private static final Logger log = Logger.get(HttpCache.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ENTRY_SUFFIX = ".entry";
    private static final String TEMPORARY_SUFFIX = ".tmp";
    private static final Duration STALE_TEMPORARY_FILE_AGE = Duration.ofHours(1);
    private static final String KEY_SEPARATOR = "-";
    // they only say how the request should be served, not which response it gets
    private static final Set<HeaderName> CACHE_DIRECTIVE_HEADERS = Set.of(CACHE_CONTROL, PRAGMA);
    private static final Set<PosixFilePermission> OWNER_ONLY_DIRECTORY = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> OWNER_ONLY_FILE = PosixFilePermissions.fromString("rw-------");
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    // evicting below the limit avoids sorting all entries again on every following write
    private static final double EVICTION_TARGET = 0.9;
    // updating file times on every read would turn reads into writes
    private static final long ACCESS_TIME_PRECISION_MILLIS = 60_000;

    private final Path directory;
    private final long maxSizeInBytes;
    private final long maxEntrySizeInBytes;
    private final Map<String, Entry> entries;
    private final Object lock;
    private final String partition;

    private record Entry(long sizeInBytes, long lastAccessMillis, long storedAccessMillis) {}

    private record Metadata(
            int statusCode,
            List<CachedResponse.Header> headers,
            Map<String, String> varyingRequestHeaders,
            long requestTimeMillis,
            long responseTimeMillis) {}

    public HttpCache(Path directory, DataSize maxSize, DataSize maxEntrySize)
    {
        this.directory = requireNonNull(directory, "directory is null");
        this.maxSizeInBytes = maxSize.toBytes();
        this.maxEntrySizeInBytes = maxEntrySize.toBytes();
        this.entries = new ConcurrentHashMap<>();
        this.lock = new Object();
        this.partition = "";
        try {
            createDirectory();
            loadEntries();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open the HTTP cache directory " + directory, e);
        }
    }

    private HttpCache(HttpCache cache, String partition)
    {
        this.directory = cache.directory;
        this.maxSizeInBytes = cache.maxSizeInBytes;
        this.maxEntrySizeInBytes = cache.maxEntrySizeInBytes;
        this.entries = cache.entries;
        this.lock = cache.lock;
        this.partition = partition;
    }

    /**
     * Returns a view of this cache that never serves responses stored in other partitions.
     * The name must not contain a new line, and it is part of the file names, so it must not reveal credentials.
     */
    public HttpCache partition(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(name.indexOf('\n') < 0, "name contains a new line");
        return new HttpCache(this, name);
    }

    public Optional<CachedResponse> get(Request request)
    {
        String key = key(request);
        if (!entries.containsKey(key)) {
            return Optional.empty();
        }
        Path file = entryFile(key);
        try {
            CachedResponse response = read(file);
            touch(key, file);
            return Optional.of(response);
        }
        catch (NoSuchFileException e) {
            entries.remove(key);
            return Optional.empty();
        }
        catch (IOException e) {
            // a corrupted or incompatible entry is just a cache miss
            log.warn(e, "Removing unreadable HTTP cache entry %s", file);
            remove(request);
            return Optional.empty();
        }
    }

    public void put(Request request, CachedResponse response)
    {
        synchronized (lock) {
            String key = key(request);
            Path file = entryFile(key);
            // other processes can share the directory, and write the same entry at the same time
            Path temporaryFile = directory.resolve(key + "." + UUID.randomUUID() + TEMPORARY_SUFFIX);
            try {
                byte[] metadata = OBJECT_MAPPER.writeValueAsBytes(new Metadata(
                        response.statusCode(),
                        response.headers(),
                        response.varyingRequestHeaders(),
                        response.requestTimeMillis(),
                        response.responseTimeMillis()));
                long size = (long) HEADER_SIZE + metadata.length + response.body().length;
                if (size > maxEntrySizeInBytes) {
                    remove(request);
                    return;
                }
                write(temporaryFile, metadata, response.body());
                Files.move(temporaryFile, file, ATOMIC_MOVE, REPLACE_EXISTING);
                long now = System.currentTimeMillis();
                entries.put(key, new Entry(size, now, now));
            }
            catch (IOException e) {
                log.warn(e, "Failed to store an HTTP cache entry for %s %s", request.getMethod(), request.getUri());
                deleteQuietly(temporaryFile);
                return;
            }
            evict();
        }
    }

    public void remove(Request request)
    {
        String key = key(request);
        synchronized (lock) {
            entries.remove(key);
            deleteQuietly(entryFile(key));
        }
    }

    public void removeAll(String method, URI uri)
    {
        synchronized (lock) {
            String keyPrefix = resourceKey(method, uri) + KEY_SEPARATOR;
            for (String key : entries.keySet().stream().filter(key -> key.startsWith(keyPrefix)).toList()) {
                entries.remove(key);
                deleteQuietly(entryFile(key));
            }
        }
    }

    long sizeInBytes()
    {
        return entries.values().stream().mapToLong(Entry::sizeInBytes).sum();
    }

    private void evict()
    {
        long size = sizeInBytes();
        if (size <= maxSizeInBytes) {
            return;
        }
        long targetSize = (long) (maxSizeInBytes * EVICTION_TARGET);
        for (Map.Entry<String, Entry> entry : entries.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getValue().lastAccessMillis()))
                .toList()) {
            if (size <= targetSize) {
                return;
            }
            entries.remove(entry.getKey());
            deleteQuietly(entryFile(entry.getKey()));
            size -= entry.getValue().sizeInBytes();
        }
    }

    // stored responses are as sensitive as the API they came from, so only the owner of the Trino process can read them
    private void createDirectory()
            throws IOException
    {
        if (!isPosix()) {
            Files.createDirectories(directory);
            return;
        }
        Files.createDirectories(directory, PosixFilePermissions.asFileAttribute(OWNER_ONLY_DIRECTORY));
        // the directory might have been created before with the default permissions
        Files.setPosixFilePermissions(directory, OWNER_ONLY_DIRECTORY);
    }

    private void loadEntries()
            throws IOException
    {
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                try {
                    if (name.endsWith(TEMPORARY_SUFFIX)) {
                        deleteIfStale(file);
                        continue;
                    }
                    if (!name.endsWith(ENTRY_SUFFIX)) {
                        continue;
                    }
                    String key = name.substring(0, name.length() - ENTRY_SUFFIX.length());
                    long accessMillis = Files.getLastModifiedTime(file).toMillis();
                    entries.put(key, new Entry(Files.size(file), accessMillis, accessMillis));
                }
                catch (NoSuchFileException e) {
                    // moved or removed by another process sharing the directory
                }
            }
        }
        evict();
    }

    // a temporary file is left by a write that didn't finish, but it can also be a write still in progress in another process
    private static void deleteIfStale(Path temporaryFile)
            throws IOException
    {
        if (System.currentTimeMillis() - Files.getLastModifiedTime(temporaryFile).toMillis() < STALE_TEMPORARY_FILE_AGE.toMillis()) {
            return;
        }
        deleteQuietly(temporaryFile);
    }

    private void touch(String key, Path file)
    {
        long now = System.currentTimeMillis();
        Entry entry = entries.computeIfPresent(key, (_, current) -> new Entry(
                current.sizeInBytes(),
                now,
                now - current.storedAccessMillis() < ACCESS_TIME_PRECISION_MILLIS ? current.storedAccessMillis() : now));
        if (entry == null || entry.storedAccessMillis() != now) {
            return;
        }
        try {
            // keep the access order after a restart
            Files.setLastModifiedTime(file, FileTime.fromMillis(now));
        }
        catch (IOException e) {
            log.debug(e, "Failed to update the access time of %s", file);
        }
    }

    private void write(Path file, byte[] metadata, byte[] body)
            throws IOException
    {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE)
                .putInt(FORMAT_VERSION)
                .putInt(metadata.length)
                .flip();
        ByteBuffer[] buffers = {header, ByteBuffer.wrap(metadata), ByteBuffer.wrap(body)};
        Set<OpenOption> options = Set.of(CREATE_NEW, WRITE);
        FileAttribute<?>[] attributes = isPosix() ? new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(OWNER_ONLY_FILE)} : new FileAttribute<?>[0];
        try (FileChannel channel = FileChannel.open(file, options, attributes)) {
            while (buffers[2].hasRemaining()) {
                channel.write(buffers);
            }
        }
    }

    private static CachedResponse read(Path file)
            throws IOException
    {
        try (FileChannel channel = FileChannel.open(file, READ)) {
            ByteBuffer header = readFully(channel, ByteBuffer.allocate(HEADER_SIZE));
            int version = header.getInt();
            if (version != FORMAT_VERSION) {
                throw new IOException("Unsupported format version " + version);
            }
            int metadataLength = header.getInt();
            long bodyLength = channel.size() - HEADER_SIZE - metadataLength;
            if (metadataLength < 0 || bodyLength < 0 || bodyLength > Integer.MAX_VALUE) {
                throw new IOException("Invalid entry sizes");
            }
            byte[] metadataBytes = new byte[metadataLength];
            readFully(channel, ByteBuffer.wrap(metadataBytes));
            byte[] body = new byte[(int) bodyLength];
            readFully(channel, ByteBuffer.wrap(body));
            Metadata metadata = OBJECT_MAPPER.readValue(metadataBytes, Metadata.class);
            return new CachedResponse(
                    metadata.statusCode(),
                    metadata.headers(),
                    metadata.varyingRequestHeaders(),
                    metadata.requestTimeMillis(),
                    metadata.responseTimeMillis(),
                    body);
        }
    }

    private static ByteBuffer readFully(FileChannel channel, ByteBuffer buffer)
            throws IOException
    {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new EOFException("Truncated entry");
            }
        }
        return buffer.flip();
    }

    private boolean isPosix()
    {
        return directory.getFileSystem().supportedFileAttributeViews().contains("posix");
    }

    private Path entryFile(String key)
    {
        return directory.resolve(key + ENTRY_SUFFIX);
    }

    private String key(Request request)
    {
        Hasher headersHasher = Hashing.sha256().newHasher();
        // every header line contains a NUL, so a partition line can't be mistaken for one, and keys without a partition don't change
        if (!partition.isEmpty()) {
            headersHasher.putString(partition, UTF_8).putByte((byte) '\n');
        }
        request.getHeaders().asMap().entrySet().stream()
                .filter(entry -> !CACHE_DIRECTIVE_HEADERS.contains(entry.getKey()))
                .sorted(Comparator.comparing(entry -> entry.getKey().toString().toLowerCase(Locale.ENGLISH)))
                .forEach(entry -> {
                    headersHasher.putString(entry.getKey().toString().toLowerCase(Locale.ENGLISH), UTF_8).putByte((byte) 0);
                    // header values can't contain NUL, so it separates them unambiguously
                    entry.getValue().forEach(value -> headersHasher.putString(value, UTF_8).putByte((byte) 0));
                    headersHasher.putByte((byte) '\n');
                });
        return resourceKey(request.getMethod(), request.getUri()) + KEY_SEPARATOR + headersHasher.hash();
    }

    private static String resourceKey(String method, URI uri)
    {
        return Hashing.sha256().hashString(method + " " + uri, UTF_8).toString();
    }

    private static void deleteQuietly(Path file)
    {
        try {
            Files.deleteIfExists(file);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete %s", file);
        }
    }
}
