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

import java.util.Locale;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record ProbeLimit(OptionalInt limit)
{
    public static final ProbeLimit NONE = new ProbeLimit(OptionalInt.empty());

    private static final String NONE_VALUE = "none";

    public ProbeLimit
    {
        requireNonNull(limit, "limit is null");
        limit.ifPresent(value -> checkArgument(value >= 0, "probe limit must not be negative"));
    }

    public static ProbeLimit fromString(String value)
    {
        requireNonNull(value, "value is null");
        if (value.trim().toLowerCase(Locale.ENGLISH).equals(NONE_VALUE)) {
            return NONE;
        }
        try {
            return new ProbeLimit(OptionalInt.of(Integer.parseInt(value.trim())));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid probe limit: '%s', must be a number or %s".formatted(value, NONE_VALUE), e);
        }
    }

    @Override
    public String toString()
    {
        return limit.isPresent() ? String.valueOf(limit.orElseThrow()) : NONE_VALUE;
    }
}
