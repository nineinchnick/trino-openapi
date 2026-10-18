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

package pl.net.was.cache;

import com.google.common.base.Splitter;

import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

// directives of the Cache-Control header from RFC 9111, section 5.2, that a private cache uses
record CacheControl(boolean noStore, boolean noCache, OptionalLong maxAge)
{
    private static final Splitter DIRECTIVE_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    static CacheControl parse(List<String> headerValues)
    {
        boolean noStore = false;
        boolean noCache = false;
        OptionalLong maxAge = OptionalLong.empty();
        for (String headerValue : headerValues) {
            for (String directive : DIRECTIVE_SPLITTER.split(headerValue)) {
                String name = directive.split("=", 2)[0].trim().toLowerCase(Locale.ENGLISH);
                switch (name) {
                    case "no-store" -> noStore = true;
                    // the qualified form (no-cache="field") only limits which fields can't be reused, so treat it the same
                    case "no-cache" -> noCache = true;
                    case "max-age" -> maxAge = parseSeconds(directive);
                    default -> {}
                }
            }
        }
        return new CacheControl(noStore, noCache, maxAge);
    }

    private static OptionalLong parseSeconds(String directive)
    {
        String[] parts = directive.split("=", 2);
        if (parts.length < 2) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(parts[1].trim().replace("\"", "")));
        }
        catch (NumberFormatException e) {
            // invalid values must be treated as stale, see RFC 9111, section 4.2.1
            return OptionalLong.of(0);
        }
    }
}
