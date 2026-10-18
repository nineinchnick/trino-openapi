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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class OpenApiOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle, ConnectorMergeTableHandle
{
    private final OpenApiTableHandle tableHandle;
    private final List<OpenApiColumnHandle> columns;
    private final Map<Integer, Set<OpenApiColumnHandle>> updateCaseColumns;

    @JsonCreator
    public OpenApiOutputTableHandle(
            @JsonProperty("tableHandle") OpenApiTableHandle tableHandle,
            @JsonProperty("columns") List<OpenApiColumnHandle> columns,
            @JsonProperty("updateCaseColumns") Map<Integer, Set<OpenApiColumnHandle>> updateCaseColumns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.updateCaseColumns = requireNonNull(updateCaseColumns, "updateCaseColumns is null").entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
    }

    @Override
    @JsonProperty("tableHandle")
    public OpenApiTableHandle getTableHandle()
    {
        return tableHandle;
    }

    // columns in the order of page channels, which don't include hidden columns
    @JsonProperty("columns")
    public List<OpenApiColumnHandle> getColumns()
    {
        return columns;
    }

    // columns updated by every case of a MERGE, by the case number, and an UPDATE has a single case
    @JsonProperty("updateCaseColumns")
    public Map<Integer, Set<OpenApiColumnHandle>> getUpdateCaseColumns()
    {
        return updateCaseColumns;
    }

    public Set<OpenApiColumnHandle> getUpdatedColumns(int caseNumber)
    {
        Set<OpenApiColumnHandle> updatedColumns = updateCaseColumns.get(caseNumber);
        checkArgument(updatedColumns != null, "Unknown update case number: %s", caseNumber);
        return updatedColumns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableHandle)
                .add("columns", columns)
                .add("updateCaseColumns", updateCaseColumns)
                .toString();
    }
}
