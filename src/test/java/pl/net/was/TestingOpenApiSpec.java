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

import io.airlift.http.client.testing.TestingHttpClient;
import pl.net.was.authentication.CatalogCredentials;

public final class TestingOpenApiSpec
{
    private TestingOpenApiSpec() {}

    // specs of most tests are files, which are read without the HTTP client
    public static OpenApiSpec createSpec(OpenApiConfig config)
    {
        return new OpenApiSpec(config, new CatalogCredentials(config), new TestingHttpClient(request -> {
            throw new AssertionError("Unexpected request to " + request.getUri());
        }));
    }
}
