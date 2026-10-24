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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.OpenApiAuthenticationClient;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class OpenApiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OpenApiConnector.class).in(SINGLETON);
        binder.bind(OpenApiMetadata.class).in(SINGLETON);
        binder.bind(OpenApiSplitManager.class).in(SINGLETON);
        binder.bind(OpenApiRecordSetProvider.class).in(SINGLETON);
        binder.bind(OpenApiPageSinkProvider.class).in(SINGLETON);
        binder.bind(OpenApiClient.class).in(SINGLETON);
        binder.bind(OpenApiSessionProperties.class).in(SINGLETON);
        configBinder(binder).bindConfig(OpenApiConfig.class);

        binder.bind(OpenApiSpec.class).in(SINGLETON);
        httpClientBinder(binder)
                .bindHttpClient("openApi", ForOpenApi.class)
                .withFilter(Authentication.class);

        httpClientBinder(binder).bindHttpClient("openApiAuthentication", OpenApiAuthenticationClient.class);
    }
}
