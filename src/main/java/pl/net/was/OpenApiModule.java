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
import io.airlift.units.DataSize;
import pl.net.was.authentication.AuthenticatingHttpClient;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.OpenApiAuthenticationClient;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

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
        // the parser didn't limit the size of the spec when it loaded it, and specs of large APIs exceed the default limit
        httpClientBinder(binder).bindHttpClient("openApiSpec", ForOpenApiSpec.class)
                .withConfigDefaults(config -> config.setMaxResponseContentLength(DataSize.of(1, GIGABYTE)));
        binder.bind(OpenApiServers.class).in(SINGLETON);
        binder.bind(Authentication.class).in(SINGLETON);
        // the max-response-size catalog property limits responses after they're decompressed, also the ones that are not compressed,
        // so the limit of the client, which applies to compressed bytes, would only make responses that are not compressed fail earlier
        httpClientBinder(binder).bindHttpClient("openApi", ForUnauthenticatedOpenApi.class)
                .withConfigDefaults(config -> config.setMaxResponseContentLength(DataSize.of(1, GIGABYTE)));

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
