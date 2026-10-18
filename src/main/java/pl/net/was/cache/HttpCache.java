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
import com.google.common.hash.Hashing;
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
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

// stores responses in files, so they survive restarts, and evicts the least recently used ones over the size limit;
// a file has a header with the format version and the length of the metadata, the metadata as JSON, and the raw body,
// so the body is written and read without being encoded or copied
public class HttpCache
{
    private static final Logger log = Logger.get(HttpCache.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ENTRY_SUFFIX = ".entry";
    private static final String TEMPORARY_SUFFIX = ".tmp";
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    // evicting below the limit avoids sorting all entries again on every following write
    private static final double EVICTION_TARGET = 0.9;
    // updating file times on every read would turn reads into writes
    private static final long ACCESS_TIME_PRECISION_MILLIS = 60_000;

    private final Path directory;
    private final long maxSizeInBytes;
    private final long maxEntrySizeInBytes;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

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
        try {
            Files.createDirectories(directory);
            loadEntries();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open the HTTP cache directory " + directory, e);
        }
    }

    public Optional<CachedResponse> get(String method, URI uri)
    {
        String key = key(method, uri);
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
            remove(method, uri);
            return Optional.empty();
        }
    }

    public synchronized void put(String method, URI uri, CachedResponse response)
    {
        String key = key(method, uri);
        Path file = entryFile(key);
        Path temporaryFile = directory.resolve(key + TEMPORARY_SUFFIX);
        try {
            byte[] metadata = OBJECT_MAPPER.writeValueAsBytes(new Metadata(
                    response.statusCode(),
                    response.headers(),
                    response.varyingRequestHeaders(),
                    response.requestTimeMillis(),
                    response.responseTimeMillis()));
            long size = HEADER_SIZE + metadata.length + response.body().length;
            if (size > maxEntrySizeInBytes) {
                remove(method, uri);
                return;
            }
            write(temporaryFile, metadata, response.body());
            Files.move(temporaryFile, file, ATOMIC_MOVE, REPLACE_EXISTING);
            long now = System.currentTimeMillis();
            entries.put(key, new Entry(size, now, now));
        }
        catch (IOException e) {
            log.warn(e, "Failed to store an HTTP cache entry for %s %s", method, uri);
            deleteQuietly(temporaryFile);
            return;
        }
        evict();
    }

    public synchronized void remove(String method, URI uri)
    {
        String key = key(method, uri);
        entries.remove(key);
        deleteQuietly(entryFile(key));
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

    private void loadEntries()
            throws IOException
    {
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (name.endsWith(TEMPORARY_SUFFIX)) {
                    // left by a write that didn't finish
                    deleteQuietly(file);
                    continue;
                }
                if (!name.endsWith(ENTRY_SUFFIX)) {
                    continue;
                }
                String key = name.substring(0, name.length() - ENTRY_SUFFIX.length());
                long accessMillis = Files.getLastModifiedTime(file).toMillis();
                entries.put(key, new Entry(Files.size(file), accessMillis, accessMillis));
            }
        }
        evict();
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

    private static void write(Path file, byte[] metadata, byte[] body)
            throws IOException
    {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE)
                .putInt(FORMAT_VERSION)
                .putInt(metadata.length)
                .flip();
        ByteBuffer[] buffers = {header, ByteBuffer.wrap(metadata), ByteBuffer.wrap(body)};
        try (FileChannel channel = FileChannel.open(file, CREATE, WRITE, TRUNCATE_EXISTING)) {
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

    private Path entryFile(String key)
    {
        return directory.resolve(key + ENTRY_SUFFIX);
    }

    private static String key(String method, URI uri)
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
