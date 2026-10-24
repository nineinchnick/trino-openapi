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
import io.airlift.configuration.ConfigurationMetadata;
import io.airlift.configuration.ConfigurationMetadata.AttributeMetadata;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;
import pl.net.was.authentication.AuthenticationScheme;
import pl.net.was.authentication.AuthenticationType;

import java.net.URI;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOpenApiConfig
{
    private static final Map<String, String> REQUIRED_PROPERTIES = ImmutableMap.of(
            "spec-location", "spec.yaml",
            "base-uri", "https://localhost:8080");

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenApiConfig.class)
                .setSpecLocation(null)
                .setBaseUri(null)
                .setAuthenticationType(AuthenticationType.NONE)
                .setAuthenticationScheme(AuthenticationScheme.BASIC)
                .setUsername(null)
                .setPassword(null)
                .setBearerToken(null)
                .setApiKeys("")
                .setApiKeyName(null)
                .setApiKeyValue(null)
                .setClientId(null)
                .setClientSecret(null)
                .setTokenUrl(null)
                .setAllowInsecureHttp(false)
                .setMaxRequestsPerSecond(null)
                .setMaxSplitsPerSecond(null)
                .setMaxRetries(3)
                .setRetryInitialDelay(new Duration(1, SECONDS))
                .setRetryMaxDelay(new Duration(1, MINUTES))
                .setHttpCacheDirectory(null)
                .setHttpCacheMaxSize(DataSize.of(1, GIGABYTE))
                .setHttpCacheMaxEntrySize(DataSize.of(10, MEGABYTE))
                .setDomainExpansionLimit(256)
                .setSplitStrategy(SplitStrategy.OFF)
                .setProbeLimit(ProbeLimit.NONE));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spec-location", "/etc/trino/spec.yaml")
                .put("base-uri", "https://api.example.com/v1")
                .put("authentication.type", "API_KEY")
                .put("authentication.scheme", "BEARER")
                .put("authentication.username", "user")
                .put("authentication.password", "password")
                .put("authentication.bearer-token", "token")
                .put("authentication.api-key-name", "X-API-Key")
                .put("authentication.api-key-value", "secret")
                .put("authentication.client-id", "client")
                .put("authentication.client-secret", "client-secret")
                .put("authentication.token-url", "https://auth.example.com/token")
                .put("authentication.allow-insecure-http", "true")
                .put("max-requests-per-second", "10")
                .put("max-splits-per-second", "0.5")
                .put("max-retries", "5")
                .put("retry-initial-delay", "100ms")
                .put("retry-max-delay", "30s")
                .put("http-cache.directory", "/var/cache/trino")
                .put("http-cache.max-size", "2GB")
                .put("http-cache.max-entry-size", "100kB")
                .put("domain-expansion-limit", "10")
                .put("split-strategy", "STRIDE")
                .put("probe-limit", "4")
                .buildOrThrow();

        OpenApiConfig expected = new OpenApiConfig()
                .setSpecLocation("/etc/trino/spec.yaml")
                .setBaseUri(URI.create("https://api.example.com/v1"))
                .setAuthenticationType(AuthenticationType.API_KEY)
                .setAuthenticationScheme(AuthenticationScheme.BEARER)
                .setUsername("user")
                .setPassword("password")
                .setBearerToken("token")
                .setApiKeyName("X-API-Key")
                .setApiKeyValue("secret")
                .setClientId("client")
                .setClientSecret("client-secret")
                .setTokenUrl(URI.create("https://auth.example.com/token"))
                .setAllowInsecureHttp(true)
                .setMaxRequestsPerSecond(10.0)
                .setMaxSplitsPerSecond(0.5)
                .setMaxRetries(5)
                .setRetryInitialDelay(new Duration(100, MILLISECONDS))
                .setRetryMaxDelay(new Duration(30, SECONDS))
                .setHttpCacheDirectory("/var/cache/trino")
                .setHttpCacheMaxSize(DataSize.of(2, GIGABYTE))
                .setHttpCacheMaxEntrySize(DataSize.of(100, KILOBYTE))
                .setDomainExpansionLimit(10)
                .setSplitStrategy(SplitStrategy.STRIDE)
                .setProbeLimit(new ProbeLimit(OptionalInt.of(4)));

        // can't be combined with api-key-name and api-key-value, so it's tested separately
        assertFullMapping(properties, expected, Set.of("authentication.api-keys"));
    }

    @Test
    void testSecuritySensitiveProperties()
    {
        assertThat(ConfigurationMetadata.getConfigurationMetadata(OpenApiConfig.class).getAttributes().values().stream()
                .filter(AttributeMetadata::isSecuritySensitive)
                .map(attribute -> attribute.getInjectionPoint().getProperty()))
                .containsExactlyInAnyOrder(
                        "spec-location",
                        "authentication.password",
                        "authentication.bearer-token",
                        "authentication.api-keys",
                        "authentication.api-key-value",
                        "authentication.client-secret");
    }

    @Test
    void testCredentialsOverHttp()
    {
        Map<String, String> properties = Map.of(
                "spec-location", "spec.yaml",
                "base-uri", "http://localhost:8080",
                "authentication.bearer-token", "token");
        assertThatThrownBy(() -> buildConfig(properties))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("base-uri and authentication.token-url must use HTTPS when credentials are configured, unless authentication.allow-insecure-http is set");
        assertThat(buildConfig(ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("authentication.allow-insecure-http", "true")
                .buildOrThrow())
                .isAllowInsecureHttp())
                .isTrue();
        assertThatThrownBy(() -> buildConfig(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.client-secret", "secret")
                .put("authentication.token-url", "http://auth.example.com/token")
                .buildOrThrow()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("must use HTTPS");
        assertThat(buildConfig(Map.of(
                "spec-location", "spec.yaml",
                "base-uri", "http://localhost:8080"))
                .getBaseUri())
                .isEqualTo(URI.create("http://localhost:8080"));
    }

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

    @Test
    void testOAuthAuthenticationType()
    {
        assertThatThrownBy(() -> buildConfig(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("authentication.type", "oauth")
                .buildOrThrow()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("authentication.type cannot be oauth, because OAuth needs the token URL and flows of an oauth2 security scheme declared in the specification");
    }

    @Test
    void testRateLimitsMustBePositive()
    {
        assertThatThrownBy(() -> buildConfig(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("max-requests-per-second", "0")
                .buildOrThrow()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property max-requests-per-second: must be greater than 0");
        assertThatThrownBy(() -> buildConfig(ImmutableMap.<String, String>builder()
                .putAll(REQUIRED_PROPERTIES)
                .put("max-splits-per-second", "-1")
                .buildOrThrow()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property max-splits-per-second: must be greater than 0");
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
