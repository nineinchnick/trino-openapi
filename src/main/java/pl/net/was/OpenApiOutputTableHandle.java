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
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OpenApiOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle, ConnectorMergeTableHandle
{
    private final OpenApiTableHandle tableHandle;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    @JsonCreator
    public OpenApiOutputTableHandle(
            @JsonProperty("tableHandle") OpenApiTableHandle tableHandle,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @Override
    @JsonProperty("tableHandle")
    public OpenApiTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableHandle)
                .toString();
    }
}
