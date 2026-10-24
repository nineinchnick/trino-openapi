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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.airlift.slice.SizeOf;
import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// splits of a scan usually start at about the same time, so they delay their first requests by different times,
// instead of sending them all at once
public record SplitRequestRate(double requestsPerSecond, Duration firstRequestDelay)
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(SplitRequestRate.class) + SizeOf.instanceSize(Duration.class);

    public SplitRequestRate
    {
        checkArgument(requestsPerSecond > 0, "requestsPerSecond must be positive");
        requireNonNull(firstRequestDelay, "firstRequestDelay is null");
    }

    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
