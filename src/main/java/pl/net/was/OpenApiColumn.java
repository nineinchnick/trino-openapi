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
import com.google.common.collect.ImmutableSet;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

public class OpenApiColumn
{
    private final String name;
    private final String sourceName;
    private final Type type;
    private final Schema<?> sourceType;
    private final Map<HttpPath, ParameterLocation> requiresPredicate;
    private final Map<HttpPath, ParameterLocation> optionalPredicate;
    private final Map<HttpPath, ParameterStyle> parameterStyles;
    private final Set<HttpPath> responseRequiredPaths;
    private final Map<HttpPath, RangeFilter> rangeFilters;
    private final Map<HttpPath, Object> parameterDefaults;
    private final ColumnMetadata metadata;
    private final boolean isPageNumber;
    private final boolean isRequestOnly;
    private final OpenApiColumnHandle handle;

    private OpenApiColumn(
            String name,
            String sourceName,
            Type type,
            Schema<?> sourceType,
            Map<HttpPath, ParameterLocation> requiresPredicate,
            Map<HttpPath, ParameterLocation> optionalPredicate,
            Map<HttpPath, ParameterStyle> parameterStyles,
            Set<HttpPath> responseRequiredPaths,
            Map<HttpPath, RangeFilter> rangeFilters,
            Map<HttpPath, Object> parameterDefaults,
            boolean isNullable,
            boolean isHidden,
            boolean isPageNumber,
            boolean isRequestOnly,
            String comment)
    {
        this.name = name;
        this.sourceName = sourceName;
        this.type = type;
        this.sourceType = sourceType;
        this.requiresPredicate = ImmutableMap.copyOf(requiresPredicate);
        this.optionalPredicate = ImmutableMap.copyOf(optionalPredicate);
        this.parameterStyles = ImmutableMap.copyOf(parameterStyles);
        this.responseRequiredPaths = ImmutableSet.copyOf(responseRequiredPaths);
        this.rangeFilters = ImmutableMap.copyOf(rangeFilters);
        this.parameterDefaults = ImmutableMap.copyOf(parameterDefaults);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(isNullable)
                .setHidden(isHidden)
                .setComment(Optional.ofNullable(comment))
                .build();
        this.isPageNumber = isPageNumber;
        this.isRequestOnly = isRequestOnly;
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

    public Map<HttpPath, ParameterLocation> getRequiresPredicate()
    {
        return requiresPredicate;
    }

    public Map<HttpPath, ParameterLocation> getOptionalPredicate()
    {
        return optionalPredicate;
    }

    public Map<HttpPath, ParameterStyle> getParameterStyles()
    {
        return parameterStyles;
    }

    /**
     * Operations whose responses always include a non-null value of this field.
     */
    public Set<HttpPath> getResponseRequiredPaths()
    {
        return responseRequiredPaths;
    }

    /**
     * Unlike the column metadata, which allows nulls so that INSERT statements can omit response fields,
     * this tells whether every row read using any of the given operations has a value.
     */
    public boolean isResponseRequired(Collection<HttpPath> selectPaths)
    {
        return !selectPaths.isEmpty() && responseRequiredPaths.containsAll(selectPaths);
    }

    public Map<HttpPath, RangeFilter> getRangeFilters()
    {
        return rangeFilters;
    }

    /**
     * Native values of this parameter sent by operations when the query doesn't filter on it.
     */
    public Map<HttpPath, Object> getParameterDefaults()
    {
        return parameterDefaults;
    }

    public ColumnMetadata getMetadata()
    {
        return metadata;
    }

    public boolean isPageNumber()
    {
        return isPageNumber;
    }

    /**
     * Request parameters and body fields that aren't also a response field, like ones renamed with the {@code _req} suffix,
     * so rows can only get their values from predicates.
     */
    public boolean isRequestOnly()
    {
        return isRequestOnly;
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

    @Override
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
                && Objects.equals(parameterStyles, that.parameterStyles)
                && Objects.equals(responseRequiredPaths, that.responseRequiredPaths)
                && Objects.equals(rangeFilters, that.rangeFilters)
                && Objects.equals(parameterDefaults, that.parameterDefaults)
                && Objects.equals(metadata, that.metadata)
                && isPageNumber == that.isPageNumber
                && isRequestOnly == that.isRequestOnly;
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
                ", parameterStyles=" + parameterStyles +
                ", responseRequiredPaths=" + responseRequiredPaths +
                ", rangeFilters=" + rangeFilters +
                ", parameterDefaults=" + parameterDefaults +
                ", metadata=" + metadata +
                ", isPageNumber=" + isPageNumber +
                ", isRequestOnly=" + isRequestOnly +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, sourceName, type, sourceType, requiresPredicate, optionalPredicate, parameterStyles, responseRequiredPaths, rangeFilters, parameterDefaults, metadata, isPageNumber, isRequestOnly);
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
        private final SortedMap<HttpPath, ParameterLocation> requiresPredicate = new TreeMap<>();
        private final SortedMap<HttpPath, ParameterLocation> optionalPredicate = new TreeMap<>();
        private final SortedMap<HttpPath, ParameterStyle> parameterStyles = new TreeMap<>();
        private final SortedSet<HttpPath> responseRequiredPaths = new TreeSet<>();
        private final SortedMap<HttpPath, RangeFilter> rangeFilters = new TreeMap<>();
        private final SortedMap<HttpPath, Object> parameterDefaults = new TreeMap<>();
        private boolean isNullable;
        private boolean isHidden;
        private boolean isPageNumber;
        private boolean isRequestOnly;
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
            this.parameterStyles.putAll(handle.getParameterStyles());
            this.responseRequiredPaths.addAll(handle.getResponseRequiredPaths());
            this.rangeFilters.putAll(handle.getRangeFilters());
            this.parameterDefaults.putAll(handle.getParameterDefaults());
            this.isNullable = handle.getMetadata().isNullable();
            this.isHidden = handle.getMetadata().isHidden();
            this.isPageNumber = handle.isPageNumber();
            this.isRequestOnly = handle.isRequestOnly();
            this.comment = handle.getMetadata().getComment().orElse(null);
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

        public OpenApiColumn.Builder setRequiresPredicate(Map<HttpPath, ParameterLocation> requiresPredicate)
        {
            this.requiresPredicate.putAll(requireNonNull(requiresPredicate, "requiresPredicate is null"));
            return this;
        }

        public OpenApiColumn.Builder setOptionalPredicate(Map<HttpPath, ParameterLocation> optionalPredicate)
        {
            this.optionalPredicate.putAll(requireNonNull(optionalPredicate, "optionalPredicate is null"));
            return this;
        }

        public OpenApiColumn.Builder setParameterStyles(Map<HttpPath, ParameterStyle> parameterStyles)
        {
            this.parameterStyles.putAll(requireNonNull(parameterStyles, "parameterStyles is null"));
            return this;
        }

        public OpenApiColumn.Builder setResponseRequiredPaths(Set<HttpPath> responseRequiredPaths)
        {
            this.responseRequiredPaths.clear();
            this.responseRequiredPaths.addAll(requireNonNull(responseRequiredPaths, "responseRequiredPaths is null"));
            return this;
        }

        public OpenApiColumn.Builder setRangeFilters(Map<HttpPath, RangeFilter> rangeFilters)
        {
            this.rangeFilters.putAll(requireNonNull(rangeFilters, "rangeFilters is null"));
            return this;
        }

        public OpenApiColumn.Builder setParameterDefaults(Map<HttpPath, Object> parameterDefaults)
        {
            this.parameterDefaults.putAll(requireNonNull(parameterDefaults, "parameterDefaults is null"));
            return this;
        }

        public OpenApiColumn.Builder setIsNullable(boolean isNullable)
        {
            this.isNullable = isNullable;
            return this;
        }

        public OpenApiColumn.Builder setIsHidden(boolean isHidden)
        {
            this.isHidden = isHidden;
            return this;
        }

        public OpenApiColumn.Builder setIsPageNumber(boolean isPageNumber)
        {
            this.isPageNumber = isPageNumber;
            return this;
        }

        public OpenApiColumn.Builder setIsRequestOnly(boolean isRequestOnly)
        {
            this.isRequestOnly = isRequestOnly;
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
                    parameterStyles,
                    responseRequiredPaths,
                    rangeFilters,
                    parameterDefaults,
                    isNullable,
                    isHidden,
                    isPageNumber,
                    isRequestOnly,
                    comment);
        }
    }
}
