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

import io.airlift.units.DataSize;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

// fails instead of returning a truncated body, which could still be valid JSON, like an array of fewer rows
final class MaxSizeInputStream
        extends FilterInputStream
{
    private final DataSize maxSize;
    private long remainingBytes;

    public MaxSizeInputStream(InputStream delegate, DataSize maxSize)
    {
        super(requireNonNull(delegate, "delegate is null"));
        this.maxSize = requireNonNull(maxSize, "maxSize is null");
        this.remainingBytes = maxSize.toBytes();
    }

    @Override
    public int read()
            throws IOException
    {
        int value = super.read();
        if (value != -1) {
            consumed(1);
        }
        return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        int read = super.read(buffer, offset, length);
        if (read > 0) {
            consumed(read);
        }
        return read;
    }

    @Override
    public long skip(long count)
            throws IOException
    {
        long skipped = super.skip(count);
        consumed(skipped);
        return skipped;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    private void consumed(long count)
            throws ResponseTooLargeException
    {
        remainingBytes -= count;
        if (remainingBytes < 0) {
            throw new ResponseTooLargeException(maxSize);
        }
    }
}
