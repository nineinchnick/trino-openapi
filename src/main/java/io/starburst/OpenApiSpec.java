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

package io.starburst;

import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.core.models.SwaggerParseResult;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class OpenApiSpec
{
    private final OpenAPI openApi;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this.openApi = requireNonNull(parse(config.getSpecLocation()), "openApi is null");

        // TODO build a list of tables and columns
    }

    private static OpenAPI parse(String specLocation)
    {
        SwaggerParseResult result = new OpenAPIParser().readLocation(specLocation, null, null);
        OpenAPI openAPI = result.getOpenAPI();

        if (result.getMessages() != null) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }

        return openAPI;
    }
}
