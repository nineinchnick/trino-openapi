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
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class OpenApiTableHandle
        implements ConnectorTableHandle, Cloneable
{
    private final SchemaTableName schemaTableName;
    private final String selectPath;
    private final PathItem.HttpMethod selectMethod;
    private final String insertPath;
    private final PathItem.HttpMethod insertMethod;
    private final String updatePath;
    private final PathItem.HttpMethod updateMethod;
    private final String deletePath;
    private final PathItem.HttpMethod deleteMethod;
    private TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenApiTableHandle(
            SchemaTableName schemaTableName,
            String selectPath,
            PathItem.HttpMethod selectMethod,
            String insertPath,
            PathItem.HttpMethod insertMethod,
            String updatePath,
            PathItem.HttpMethod updateMethod,
            String deletePath,
            PathItem.HttpMethod deleteMethod,
            TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = schemaTableName;
        this.selectPath = selectPath;
        this.selectMethod = selectMethod;
        this.insertPath = insertPath;
        this.insertMethod = insertMethod;
        this.updatePath = updatePath;
        this.updateMethod = updateMethod;
        this.deletePath = deletePath;
        this.deleteMethod = deleteMethod;
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
    public PathItem.HttpMethod getSelectMethod()
    {
        return selectMethod;
    }

    @JsonProperty
    public String getInsertPath()
    {
        return insertPath;
    }

    @JsonProperty
    public PathItem.HttpMethod getInsertMethod()
    {
        return insertMethod;
    }

    @JsonProperty
    public String getUpdatePath()
    {
        return updatePath;
    }

    @JsonProperty
    public PathItem.HttpMethod getUpdateMethod()
    {
        return updateMethod;
    }

    @JsonProperty
    public String getDeletePath()
    {
        return deletePath;
    }

    @JsonProperty
    public PathItem.HttpMethod getDeleteMethod()
    {
        return deleteMethod;
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
