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
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import pl.net.was.authentication.AuthenticatingHttpClient;
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
        binder.bind(OpenApiServers.class).in(SINGLETON);
        binder.bind(Authentication.class).in(SINGLETON);
        httpClientBinder(binder).bindHttpClient("openApi", ForUnauthenticatedOpenApi.class);

        httpClientBinder(binder).bindHttpClient("openApiAuthentication", OpenApiAuthenticationClient.class);
    }

    // the wrapped client is bound by the HTTP client binder, which closes it when the connector shuts down
    @SuppressWarnings("CloseableProvides")
    @Provides
    @Singleton
    @ForOpenApi
    public static HttpClient createHttpClient(@ForUnauthenticatedOpenApi HttpClient httpClient, Authentication authentication)
    {
        return new AuthenticatingHttpClient(httpClient, authentication);
    }
}
