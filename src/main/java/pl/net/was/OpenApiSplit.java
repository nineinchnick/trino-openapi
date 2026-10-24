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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OpenApiSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(OpenApiSplit.class);

    private final OpenApiTableHandle tableHandle;
    private final int firstPage;
    private final int pageIncrement;
    private final Optional<SplitRequestRate> requestRate;

    public OpenApiSplit(OpenApiTableHandle tableHandle)
    {
        this(tableHandle, 1, 1);
    }

    public OpenApiSplit(OpenApiTableHandle tableHandle, int firstPage, int pageIncrement)
    {
        this(tableHandle, firstPage, pageIncrement, Optional.empty());
    }

    // Trino can't deserialize splits using a private creator
    @JsonCreator
    public OpenApiSplit(
            @JsonProperty("tableHandle") OpenApiTableHandle tableHandle,
            @JsonProperty("firstPage") int firstPage,
            @JsonProperty("pageIncrement") int pageIncrement,
            @JsonProperty("requestRate") Optional<SplitRequestRate> requestRate)
    {
        checkArgument(firstPage > 0, "firstPage must be positive");
        checkArgument(pageIncrement > 0, "pageIncrement must be positive");
        this.tableHandle = tableHandle;
        this.firstPage = firstPage;
        this.pageIncrement = pageIncrement;
        this.requestRate = requireNonNull(requestRate, "requestRate is null");
    }

    public OpenApiSplit withRequestRate(SplitRequestRate requestRate)
    {
        return new OpenApiSplit(tableHandle, firstPage, pageIncrement, Optional.of(requestRate));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @JsonProperty("tableHandle")
    public OpenApiTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty("firstPage")
    public int getFirstPage()
    {
        return firstPage;
    }

    @JsonProperty("pageIncrement")
    public int getPageIncrement()
    {
        return pageIncrement;
    }

    @JsonProperty("requestRate")
    public Optional<SplitRequestRate> getRequestRate()
    {
        return requestRate;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return (long) INSTANCE_SIZE
                + tableHandle.getRetainedSizeInBytes()
                + SizeOf.sizeOf(requestRate, SplitRequestRate::getRetainedSizeInBytes);
    }
}
