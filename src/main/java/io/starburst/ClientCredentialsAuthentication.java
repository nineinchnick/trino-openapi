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
package io.starburst;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Suppliers;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;

import javax.inject.Inject;

import java.net.URI;
import java.util.function.Supplier;

import static com.google.common.io.BaseEncoding.base64Url;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ClientCredentialsAuthentication
        implements Authentication
{
    private final URI baseUri;
    private final String clientId;
    private final String clientSecret;
    private final HttpClient httpClient;

    private final Supplier<String> token = Suppliers.memoize(this::getToken);

    @Inject
    public ClientCredentialsAuthentication(
            OpenApiConfig config,
            ClientCredentialsAuthenticationConfig authenticationConfig,
            @OpenApiAuthenticationClient HttpClient httpClient)
    {
        this.baseUri = requireNonNull(config.getBaseUri(), "baseUri is null");
        this.clientId = requireNonNull(authenticationConfig.getClientId(), "clientId is null");
        this.clientSecret = requireNonNull(authenticationConfig.getClientSecret(), "clientSecret is null");
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
                                        .appendPath("/oauth/v2/token")
                                        .addParameter("token_format", "oauth2")
                                        .build())
                                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                                .setHeader("Authorization", "Basic " + base64Url().encode("%s:%s".formatted(clientId, clientSecret).getBytes(UTF_8)))
                                .setBodyGenerator(out -> out.write("grant_type=client_credentials".getBytes(UTF_8)))
                                .build(),
                        createJsonResponseHandler(jsonCodec(TokenResponse.class)))
                .accessToken();
    }

    public record TokenResponse(
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("access_token") String accessToken) {}
}
