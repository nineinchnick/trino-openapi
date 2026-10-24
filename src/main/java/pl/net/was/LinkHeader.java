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

import com.google.common.base.Splitter;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads links from the values of the {@code Link} header, described in <a href="https://www.rfc-editor.org/rfc/rfc8288">RFC 8288</a>.
 */
final class LinkHeader
{
    // parameter values can be quoted strings, which can contain commas and semicolons
    private static final String PARAMETER = ";\\s*([^;,=\\s]+)\\s*(?:=\\s*(?:\"([^\"]*)\"|([^;,\"\\s]*)))?\\s*";
    private static final Pattern LINK_VALUE = Pattern.compile("<([^>]*)>\\s*((?:" + PARAMETER + ")*)");
    private static final Pattern LINK_PARAMETER = Pattern.compile(PARAMETER);
    private static final Splitter RELATION_SPLITTER = Splitter.on(Pattern.compile("\\s+")).omitEmptyStrings();

    private LinkHeader() {}

    public static Optional<String> getNextLink(List<String> headerValues)
    {
        for (String headerValue : headerValues) {
            Matcher linkValue = LINK_VALUE.matcher(headerValue);
            while (linkValue.find()) {
                if (hasNextRelation(linkValue.group(2))) {
                    return Optional.of(linkValue.group(1));
                }
            }
        }
        return Optional.empty();
    }

    private static boolean hasNextRelation(String parameters)
    {
        Matcher parameter = LINK_PARAMETER.matcher(parameters);
        while (parameter.find()) {
            if (!parameter.group(1).equalsIgnoreCase("rel")) {
                continue;
            }
            String relations = parameter.group(2) != null ? parameter.group(2) : parameter.group(3);
            // the relation can be a list of relation types, which are compared ignoring case
            return relations != null && RELATION_SPLITTER.splitToStream(relations)
                    .anyMatch(relation -> relation.toLowerCase(Locale.ENGLISH).equals("next"));
        }
        return false;
    }
}
