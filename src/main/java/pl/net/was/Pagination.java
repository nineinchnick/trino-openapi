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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record Pagination(
        Optional<String> limitParam,
        JsonPointer totalResultsPointer,
        JsonPointer totalPagesPointer,
        JsonPointer hasMorePointer,
        OptionalInt maxPages,
        OptionalLong maxResults,
        Optional<Sorting> sorting,
        Optional<SplitStrategy> splitStrategy,
        OptionalInt strideSplits,
        Optional<ProbeLimit> probeLimit)
{
    private static final Pagination NONE = builder().build();

    public Pagination
    {
        requireNonNull(limitParam, "limitParam is null");
        requireNonNull(totalResultsPointer, "totalResultsPointer is null");
        requireNonNull(totalPagesPointer, "totalPagesPointer is null");
        requireNonNull(hasMorePointer, "hasMorePointer is null");
        requireNonNull(maxPages, "maxPages is null");
        maxPages.ifPresent(value -> checkArgument(value > 0, "maxPages must be positive"));
        requireNonNull(maxResults, "maxResults is null");
        maxResults.ifPresent(value -> checkArgument(value > 0, "maxResults must be positive"));
        requireNonNull(sorting, "sorting is null");
        requireNonNull(splitStrategy, "splitStrategy is null");
        requireNonNull(strideSplits, "strideSplits is null");
        strideSplits.ifPresent(value -> checkArgument(value > 0, "strideSplits must be positive"));
        requireNonNull(probeLimit, "probeLimit is null");
    }

    public static Pagination none()
    {
        return NONE;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<String> limitParam = Optional.empty();
        private JsonPointer totalResultsPointer = JsonPointer.empty();
        private JsonPointer totalPagesPointer = JsonPointer.empty();
        private JsonPointer hasMorePointer = JsonPointer.empty();
        private OptionalInt maxPages = OptionalInt.empty();
        private OptionalLong maxResults = OptionalLong.empty();
        private Optional<Sorting> sorting = Optional.empty();
        private Optional<SplitStrategy> splitStrategy = Optional.empty();
        private OptionalInt strideSplits = OptionalInt.empty();
        private Optional<ProbeLimit> probeLimit = Optional.empty();

        private Builder() {}

        public Builder setLimitParam(Optional<String> limitParam)
        {
            this.limitParam = requireNonNull(limitParam, "limitParam is null");
            return this;
        }

        public Builder setTotalResultsPointer(JsonPointer totalResultsPointer)
        {
            this.totalResultsPointer = requireNonNull(totalResultsPointer, "totalResultsPointer is null");
            return this;
        }

        public Builder setTotalPagesPointer(JsonPointer totalPagesPointer)
        {
            this.totalPagesPointer = requireNonNull(totalPagesPointer, "totalPagesPointer is null");
            return this;
        }

        public Builder setHasMorePointer(JsonPointer hasMorePointer)
        {
            this.hasMorePointer = requireNonNull(hasMorePointer, "hasMorePointer is null");
            return this;
        }

        public Builder setMaxPages(OptionalInt maxPages)
        {
            this.maxPages = requireNonNull(maxPages, "maxPages is null");
            return this;
        }

        public Builder setMaxResults(OptionalLong maxResults)
        {
            this.maxResults = requireNonNull(maxResults, "maxResults is null");
            return this;
        }

        public Builder setSorting(Optional<Sorting> sorting)
        {
            this.sorting = requireNonNull(sorting, "sorting is null");
            return this;
        }

        public Builder setSplitStrategy(Optional<SplitStrategy> splitStrategy)
        {
            this.splitStrategy = requireNonNull(splitStrategy, "splitStrategy is null");
            return this;
        }

        public Builder setStrideSplits(OptionalInt strideSplits)
        {
            this.strideSplits = requireNonNull(strideSplits, "strideSplits is null");
            return this;
        }

        public Builder setProbeLimit(Optional<ProbeLimit> probeLimit)
        {
            this.probeLimit = requireNonNull(probeLimit, "probeLimit is null");
            return this;
        }

        public Pagination build()
        {
            return new Pagination(limitParam, totalResultsPointer, totalPagesPointer, hasMorePointer, maxPages, maxResults, sorting, splitStrategy, strideSplits, probeLimit);
        }
    }
}
