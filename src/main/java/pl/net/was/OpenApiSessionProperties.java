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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class OpenApiSessionProperties
{
    private static final String HTTP_CACHE_ENABLED = "http_cache_enabled";

    private final List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
            booleanProperty(
                    HTTP_CACHE_ENABLED,
                    "Use fresh cached HTTP responses without asking the server; when false, cached responses are revalidated",
                    true,
                    false));

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isHttpCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(HTTP_CACHE_ENABLED, Boolean.class);
    }
}
