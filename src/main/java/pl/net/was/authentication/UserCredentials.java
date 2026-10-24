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

import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Credentials of the user running a query, passed as extra credentials, which replace the ones of the catalog.
 * They must never be logged, or included in handles or splits, so this class has no JSON representation, and doesn't print them.
 */
public final class UserCredentials
{
    public static final UserCredentials NONE = new UserCredentials(Optional.empty(), Optional.empty(), Optional.empty());

    private final Optional<UsernamePassword> usernamePassword;
    private final Optional<String> bearerToken;
    private final Optional<String> apiKeyValue;

    public UserCredentials(Optional<UsernamePassword> usernamePassword, Optional<String> bearerToken, Optional<String> apiKeyValue)
    {
        this.usernamePassword = requireNonNull(usernamePassword, "usernamePassword is null");
        this.bearerToken = requireNonNull(bearerToken, "bearerToken is null");
        this.apiKeyValue = requireNonNull(apiKeyValue, "apiKeyValue is null");
    }

    public boolean isEmpty()
    {
        return usernamePassword.isEmpty() && bearerToken.isEmpty() && apiKeyValue.isEmpty();
    }

    public Optional<UsernamePassword> getUsernamePassword()
    {
        return usernamePassword;
    }

    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    public Optional<String> getApiKeyValue()
    {
        return apiKeyValue;
    }

    /**
     * Returns all values in a fixed order, separated by NUL, which HTTP headers carrying extra credentials can't contain,
     * and with a prefix for passed ones, so different credentials never give the same string.
     */
    public String toSecretString()
    {
        return Stream.of(
                        usernamePassword.map(UsernamePassword::username),
                        usernamePassword.map(UsernamePassword::password),
                        bearerToken,
                        apiKeyValue)
                .map(value -> value.map(present -> "+" + present).orElse(""))
                .collect(joining("\0"));
    }

    @Override
    public String toString()
    {
        return "UserCredentials{redacted}";
    }

    public record UsernamePassword(String username, String password)
    {
        public UsernamePassword
        {
            requireNonNull(username, "username is null");
            requireNonNull(password, "password is null");
        }

        @Override
        public String toString()
        {
            return "UsernamePassword{redacted}";
        }
    }
}
