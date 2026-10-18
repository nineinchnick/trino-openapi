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
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class OpenApiSessionProperties
{
    private static final String HTTP_CACHE_ENABLED = "http_cache_enabled";
    // without defaults, so that unset properties don't override values from the spec
    private static final String SPLIT_STRATEGY = "split_strategy";
    private static final String PROBE_LIMIT = "probe_limit";

    private final List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
            booleanProperty(
                    HTTP_CACHE_ENABLED,
                    "Use fresh cached HTTP responses without asking the server; when false, cached responses are revalidated",
                    true,
                    false),
            enumProperty(
                    SPLIT_STRATEGY,
                    "How to read pages in separate splits, overriding the spec and the split-strategy catalog property",
                    SplitStrategy.class,
                    null,
                    false),
            new PropertyMetadata<>(
                    PROBE_LIMIT,
                    "Page size of the first page requested by the probe split strategy, or none to use the regular page size, overriding the spec and the probe-limit catalog property",
                    VARCHAR,
                    ProbeLimit.class,
                    null,
                    false,
                    value -> ProbeLimit.fromString((String) value),
                    ProbeLimit::toString));

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isHttpCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(HTTP_CACHE_ENABLED, Boolean.class);
    }

    public static Optional<SplitStrategy> getSplitStrategy(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SPLIT_STRATEGY, SplitStrategy.class));
    }

    public static Optional<ProbeLimit> getProbeLimit(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(PROBE_LIMIT, ProbeLimit.class));
    }
}
