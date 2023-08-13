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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class OpenApiTableHandle
        implements ConnectorTableHandle, Cloneable
{
    private final SchemaTableName schemaTableName;
    private final String selectPath;
    private final String insertPath;
    private final String updatePath;
    private final String deletePath;
    private TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenApiTableHandle(
            SchemaTableName schemaTableName,
            String selectPath,
            String insertPath,
            String updatePath,
            String deletePath,
            TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = schemaTableName;
        this.selectPath = selectPath;
        this.insertPath = insertPath;
        this.updatePath = updatePath;
        this.deletePath = deletePath;
        this.constraint = constraint;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getSelectPath()
    {
        return selectPath;
    }

    @JsonProperty
    public String getInsertPath()
    {
        return insertPath;
    }

    @JsonProperty
    public String getUpdatePath()
    {
        return updatePath;
    }

    @JsonProperty
    public String getDeletePath()
    {
        return deletePath;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return schemaTableName.getTableName();
    }

    @Override
    public OpenApiTableHandle clone()
    {
        try {
            return (OpenApiTableHandle) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public OpenApiTableHandle cloneWithConstraint(TupleDomain<ColumnHandle> constraint)
    {
        OpenApiTableHandle tableHandle = this.clone();
        tableHandle.constraint = constraint;
        return tableHandle;
    }
}
