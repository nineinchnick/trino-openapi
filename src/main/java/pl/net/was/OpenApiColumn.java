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

import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.swagger.v3.oas.models.PathItem.HttpMethod;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiColumn
{
    private final String name;
    private final String sourceName;
    private final Type type;
    private final Schema<?> sourceType;
    private final Map<HttpMethod, String> requiresPredicate;
    private final Map<HttpMethod, String> optionalPredicate;
    private final ColumnMetadata metadata;
    private final OpenApiColumnHandle handle;

    private OpenApiColumn(String name, String sourceName, Type type, Schema<?> sourceType, Map<HttpMethod, String> requiresPredicate, Map<HttpMethod, String> optionalPredicate, boolean isNullable, String comment)
    {
        this.name = name;
        this.sourceName = sourceName;
        this.type = type;
        this.sourceType = sourceType;
        this.requiresPredicate = ImmutableMap.copyOf(requiresPredicate);
        this.optionalPredicate = ImmutableMap.copyOf(optionalPredicate);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(isNullable)
                .setHidden(name.equals(ROW_ID))
                .setComment(Optional.ofNullable(comment))
                .build();
        this.handle = new OpenApiColumnHandle(name, type);
    }

    public String getName()
    {
        return name;
    }

    public String getSourceName()
    {
        return sourceName;
    }

    public Type getType()
    {
        return type;
    }

    public Schema<?> getSourceType()
    {
        return sourceType;
    }

    public Map<HttpMethod, String> getRequiresPredicate()
    {
        return requiresPredicate;
    }

    public Map<HttpMethod, String> getOptionalPredicate()
    {
        return optionalPredicate;
    }

    public ColumnMetadata getMetadata()
    {
        return metadata;
    }

    public OpenApiColumnHandle getHandle()
    {
        return handle;
    }

    public record PrimaryKey(String name, String type) {}

    public PrimaryKey getPrimaryKey()
    {
        return new PrimaryKey(name, type.getDisplayName());
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenApiColumn that = (OpenApiColumn) o;
        return Objects.equals(name, that.name)
                && Objects.equals(sourceName, that.sourceName)
                && Objects.equals(type, that.type)
                && Objects.equals(sourceType, that.sourceType)
                && Objects.equals(requiresPredicate, that.requiresPredicate)
                && Objects.equals(optionalPredicate, that.optionalPredicate)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public String toString()
    {
        return "OpenApiColumn{" +
                "name='" + name + '\'' +
                ", sourceName='" + sourceName + '\'' +
                ", type=" + type +
                ", sourceType=" + sourceType.getType() +
                ", requiresPredicate=" + requiresPredicate +
                ", optionalPredicate=" + optionalPredicate +
                ", metadata=" + metadata +
                '}';
    }

    public int hashCode()
    {
        return Objects.hash(name, sourceName, type, sourceType, requiresPredicate, optionalPredicate, metadata);
    }

    public static OpenApiColumn.Builder builder()
    {
        return new OpenApiColumn.Builder();
    }

    public static OpenApiColumn.Builder builderFrom(OpenApiColumn handle)
    {
        return new OpenApiColumn.Builder(handle);
    }

    public static class Builder
    {
        private String name;
        private String sourceName;
        private Type type;
        private Schema<?> sourceType;
        private final SortedMap<HttpMethod, String> requiresPredicate = new TreeMap<>();
        private final SortedMap<HttpMethod, String> optionalPredicate = new TreeMap<>();
        private boolean isNullable;
        private String comment;

        private Builder() {}

        private Builder(OpenApiColumn handle)
        {
            this.name = handle.getName();
            this.sourceName = handle.getSourceName();
            this.type = handle.getType();
            this.sourceType = handle.getSourceType();
            this.requiresPredicate.putAll(handle.getRequiresPredicate());
            this.optionalPredicate.putAll(handle.getOptionalPredicate());
            this.isNullable = handle.getMetadata().isNullable();
            this.comment = handle.getMetadata().getComment();
        }

        public OpenApiColumn.Builder setName(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public OpenApiColumn.Builder setSourceName(String sourceName)
        {
            this.sourceName = requireNonNull(sourceName, "sourceName is null");
            return this;
        }

        public OpenApiColumn.Builder setType(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            return this;
        }

        public OpenApiColumn.Builder setSourceType(Schema<?> sourceType)
        {
            this.sourceType = requireNonNull(sourceType, "sourceType is null");
            return this;
        }

        public OpenApiColumn.Builder setRequiresPredicate(Map<HttpMethod, String> requiresPredicate)
        {
            this.requiresPredicate.putAll(requireNonNull(requiresPredicate, "requiresPredicate is null"));
            return this;
        }

        public OpenApiColumn.Builder setOptionalPredicate(Map<HttpMethod, String> optionalPredicate)
        {
            this.optionalPredicate.putAll(requireNonNull(optionalPredicate, "optionalPredicate is null"));
            return this;
        }

        public OpenApiColumn.Builder setIsNullable(boolean isNullable)
        {
            this.isNullable = isNullable;
            return this;
        }

        public OpenApiColumn.Builder setComment(String name)
        {
            if (name != null) {
                this.comment = name;
            }
            return this;
        }

        public OpenApiColumn build()
        {
            return new OpenApiColumn(
                    name,
                    sourceName,
                    type,
                    sourceType,
                    requiresPredicate,
                    optionalPredicate,
                    isNullable,
                    comment);
        }
    }
}
