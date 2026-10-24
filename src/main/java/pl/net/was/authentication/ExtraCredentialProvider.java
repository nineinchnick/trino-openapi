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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import pl.net.was.OpenApiConfig;
import pl.net.was.authentication.UserCredentials.UsernamePassword;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Reads the credentials of the user running a query from the extra credentials named by the catalog properties, like base-jdbc's provider of the same name.
 */
public class ExtraCredentialProvider
{
    private final Optional<String> usernameCredentialName;
    private final Optional<String> passwordCredentialName;
    private final Optional<String> bearerTokenCredentialName;
    private final Optional<String> apiKeyValueCredentialName;

    @Inject
    public ExtraCredentialProvider(OpenApiConfig config)
    {
        requireNonNull(config, "config is null");
        this.usernameCredentialName = Optional.ofNullable(config.getUsernameCredentialName());
        this.passwordCredentialName = Optional.ofNullable(config.getPasswordCredentialName());
        this.bearerTokenCredentialName = Optional.ofNullable(config.getBearerTokenCredentialName());
        this.apiKeyValueCredentialName = Optional.ofNullable(config.getApiKeyValueCredentialName());
    }

    public UserCredentials getUserCredentials(ConnectorIdentity identity)
    {
        Map<String, String> extraCredentials = identity.getExtraCredentials();
        Optional<String> username = usernameCredentialName.map(extraCredentials::get);
        Optional<String> password = passwordCredentialName.map(extraCredentials::get);
        // mixing the username of the user with the password of the catalog, or the other way around, would send credentials of different accounts
        if (username.isPresent() != password.isPresent()) {
            throw new TrinoException(GENERIC_USER_ERROR, format(
                    "The %s and %s extra credentials must be passed together",
                    usernameCredentialName.orElseThrow(),
                    passwordCredentialName.orElseThrow()));
        }
        UserCredentials credentials = new UserCredentials(
                username.map(value -> new UsernamePassword(value, password.orElseThrow())),
                bearerTokenCredentialName.map(extraCredentials::get),
                apiKeyValueCredentialName.map(extraCredentials::get));
        return credentials.isEmpty() ? UserCredentials.NONE : credentials;
    }
}
