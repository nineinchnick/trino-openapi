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
import io.trino.spi.NodeManager;
import io.trino.spi.type.TypeManager;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.OpenApiAuthenticationClient;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class OpenApiModule
        extends AbstractConfigurationAwareModule
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    public OpenApiModule(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(OpenApiConnector.class).in(SINGLETON);
        binder.bind(OpenApiMetadata.class).in(SINGLETON);
        binder.bind(OpenApiSplitManager.class).in(SINGLETON);
        binder.bind(OpenApiRecordSetProvider.class).in(SINGLETON);
        configBinder(binder).bindConfig(OpenApiConfig.class);

        binder.bind(OpenApiSpec.class).in(SINGLETON);
        httpClientBinder(binder)
                .bindHttpClient("openApi", OpenApiClient.class)
                .withFilter(Authentication.class);

        httpClientBinder(binder).bindHttpClient("openApiAuthentication", OpenApiAuthenticationClient.class);
    }
}
