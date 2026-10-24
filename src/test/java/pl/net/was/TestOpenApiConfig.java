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

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOpenApiConfig
{
    private static final Map<String, String> REQUIRED_PROPERTIES = ImmutableMap.of(
            "spec-location", "spec.yaml",
            "base-uri", "http://localhost:8080");

    @Test
    void testApiKeys()
    {
        OpenApiConfig config = buildConfig(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.api-keys", "a=1,b=2")
                .buildOrThrow());

        assertThat(config.getApiKeys()).isEqualTo(Map.of("a", "1", "b", "2"));
        assertThat(config.getApiKeyName()).isNull();
        assertThat(config.getApiKeyValue()).isNull();
    }

    @Test
    void testApiKeysCombinedWithApiKeyName()
    {
        assertFailsValidation(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.api-key-name", "X-API-Key")
                .put("authentication.api-keys", "a=1,b=2")
                .buildOrThrow());
    }

    @Test
    void testApiKeysCombinedWithApiKeyValue()
    {
        assertFailsValidation(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.api-key-value", "secret")
                .put("authentication.api-keys", "a=1,b=2")
                .buildOrThrow());
    }

    @Test
    void testApiKeysCombinedWithBlankApiKeyName()
    {
        // a blank value is set, so it can reveal mistakes like a typo in an environment variable name
        assertFailsValidation(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.api-key-name", "")
                .put("authentication.api-keys", "a=1,b=2")
                .buildOrThrow());
    }

    @Test
    void testApiKeysCombinedWithApiKeyNameAndValueInAnySetterOrder()
    {
        assertThat(new OpenApiConfig()
                .setApiKeys("a=1,b=2")
                .setApiKeyName("X-API-Key")
                .setApiKeyValue("secret")
                .isApiKeysValid())
                .isFalse();
        assertThat(new OpenApiConfig()
                .setApiKeyName("X-API-Key")
                .setApiKeyValue("secret")
                .setApiKeys("a=1,b=2")
                .isApiKeysValid())
                .isFalse();
    }

    private static void assertFailsValidation(Map<String, String> properties)
    {
        assertThatThrownBy(() -> buildConfig(properties))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("authentication.api-keys cannot be combined with authentication.api-key-name or authentication.api-key-value");
    }

    private static OpenApiConfig buildConfig(Map<String, String> properties)
    {
        return new ConfigurationFactory(properties).build(OpenApiConfig.class);
    }
}
