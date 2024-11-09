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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

public class OpenApiColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(OpenApiColumnHandle.class);

    private final String name;
    private final Type type;

    @JsonCreator
    public OpenApiColumnHandle(String name, Type type)
    {
        this.name = name;
        this.type = type;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenApiColumnHandle that = (OpenApiColumnHandle) o;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    public long getRetainedSizeInBytes()
    {
        return (long) INSTANCE_SIZE
                + SizeOf.estimatedSizeOf(this.name);
    }
}
