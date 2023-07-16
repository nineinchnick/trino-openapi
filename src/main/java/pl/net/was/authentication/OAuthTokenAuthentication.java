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
package pl.net.was.authentication;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import pl.net.was.OpenApiConfig;

import javax.inject.Inject;

import java.net.URI;
import java.net.URLEncoder;
import java.util.function.Supplier;

import static com.google.common.io.BaseEncoding.base64Url;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class OAuthTokenAuthentication
        implements Authentication
{
    private final URI baseUri;
    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final BodyGenerator bodyGenerator;
    private final HttpClient httpClient;

    private final Supplier<String> token = Suppliers.memoize(this::getToken);

    @Inject
    public OAuthTokenAuthentication(
            OpenApiConfig config,
            OAuthTokenAuthenticationConfig authenticationConfig,
            @OpenApiAuthenticationClient HttpClient httpClient)
    {
        this.baseUri = requireNonNull(config.getBaseUri(), "baseUri is null");
        this.tokenEndpoint = authenticationConfig.getTokenEndpoint();
        this.clientId = requireNonNull(authenticationConfig.getClientId(), "clientId is null");
        this.clientSecret = requireNonNull(authenticationConfig.getClientSecret(), "clientSecret is null");
        this.bodyGenerator = createStaticBodyGenerator(getBody(authenticationConfig.getGrantType(), authenticationConfig.getUsername(), authenticationConfig.getPassword()), UTF_8);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader("Authorization", "Bearer " + token.get())
                .build();
    }

    private String getToken()
    {
        return httpClient.execute(
                        preparePost()
                                .setUri(uriBuilderFrom(baseUri)
                                        .replacePath(tokenEndpoint)
                                        .build())
                                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                                .setHeader("Authorization", "Basic " + base64Url().encode("%s:%s".formatted(clientId, clientSecret).getBytes(UTF_8)))
                                .setBodyGenerator(bodyGenerator)
                                .build(),
                        createJsonResponseHandler(jsonCodec(TokenResponse.class)))
                .accessToken();
    }

    private static String getBody(String grantType, String username, String password)
    {
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                    .put("grant_type", grantType);
        if (username != null && !username.isEmpty()) {
            params.put("username", username);
        }
        if (password != null && !password.isEmpty()) {
            params.put("password", password);
        }

        return params.build().entrySet().stream()
                .map(entry -> format("%s=%s", entry.getKey(), URLEncoder.encode(entry.getValue(), UTF_8)))
                .collect(joining("&"));
    }

    public record TokenResponse(
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("access_token") String accessToken) {}
}
