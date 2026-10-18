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

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Request parameters that make the server sort results by a column, so reading only the first pages returns the top rows.
 *
 * @param sortValues values of the sort parameter, by the name of the response field they sort by
 */
public record Sorting(
        String sortParam,
        Map<String, String> sortValues,
        String directionParam,
        String ascendingValue,
        String descendingValue)
{
    public Sorting
    {
        requireNonNull(sortParam, "sortParam is null");
        checkArgument(sortValues != null && !sortValues.isEmpty(), "sortValues are required with sortParam");
        checkArgument(directionParam != null, "directionParam is required with sortParam");
        checkArgument(!directionParam.equals(sortParam), "directionParam must be different from sortParam");
        checkArgument(ascendingValue != null, "ascendingValue is required with sortParam");
        checkArgument(descendingValue != null, "descendingValue is required with sortParam");
        sortValues = ImmutableMap.copyOf(sortValues);
    }

    public String directionValue(boolean ascending)
    {
        return ascending ? ascendingValue : descendingValue;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String sortParam;
        private Map<String, String> sortValues = ImmutableMap.of();
        private String directionParam;
        private String ascendingValue;
        private String descendingValue;

        private Builder() {}

        public Builder setSortParam(String sortParam)
        {
            this.sortParam = sortParam;
            return this;
        }

        public Builder setSortValues(Map<String, String> sortValues)
        {
            this.sortValues = sortValues;
            return this;
        }

        public Builder setDirectionParam(String directionParam)
        {
            this.directionParam = directionParam;
            return this;
        }

        public Builder setAscendingValue(String ascendingValue)
        {
            this.ascendingValue = ascendingValue;
            return this;
        }

        public Builder setDescendingValue(String descendingValue)
        {
            this.descendingValue = descendingValue;
            return this;
        }

        public Sorting build()
        {
            return new Sorting(sortParam, sortValues, directionParam, ascendingValue, descendingValue);
        }
    }
}
