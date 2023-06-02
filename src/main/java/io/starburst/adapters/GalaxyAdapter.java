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

package io.starburst.adapters;

import io.starburst.OpenApiSpecAdapter;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.connector.ConnectorTableMetadata;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

public class GalaxyAdapter
        implements OpenApiSpecAdapter
{
    public Map<String, Schema> runAdapter(String operationId, Map<String, Schema> original)
    {
        if (original.containsKey("result")) {
            return original.get("result").getItems().getProperties();
        }
        return original;
    }

    public JSONArray runAdapter(ConnectorTableMetadata tableMetadata, JSONObject original)
    {
        if (original.keySet().contains("result")) {
            return original.getJSONArray("result");
        }
        return null;
    }
}