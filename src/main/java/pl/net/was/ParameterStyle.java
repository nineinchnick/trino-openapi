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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;
import com.google.common.escape.Escaper;
import com.google.common.net.PercentEscaper;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record ParameterStyle(Parameter.StyleEnum style, boolean explode, boolean allowReserved)
{
    // RFC 3986 unreserved characters, which never have to be percent-encoded
    private static final Escaper UNRESERVED_ESCAPER = new PercentEscaper("-._~", false);
    // RFC 3986 unreserved and reserved characters allowed in a query, so without `#`, `[`, and `]`
    private static final Escaper QUERY_RESERVED_ESCAPER = new PercentEscaper("-._~!$&'()*+,;=:@/?", false);
    private static final Pattern PERCENT_ENCODED_OCTET = Pattern.compile("%[0-9A-Fa-f]{2}");

    public ParameterStyle
    {
        requireNonNull(style, "style is null");
    }

    public ParameterStyle(Parameter.StyleEnum style, boolean explode)
    {
        this(style, explode, false);
    }

    public static ParameterStyle defaultFor(ParameterLocation location)
    {
        return switch (location) {
            case QUERY, COOKIE -> new ParameterStyle(Parameter.StyleEnum.FORM, true);
            default -> new ParameterStyle(Parameter.StyleEnum.SIMPLE, false);
        };
    }

    public static ParameterStyle of(Parameter.StyleEnum style, Boolean explode, Boolean allowReserved, ParameterLocation location)
    {
        Parameter.StyleEnum resolvedStyle = style == null ? defaultFor(location).style() : style;
        return new ParameterStyle(
                resolvedStyle,
                explode == null ? resolvedStyle == Parameter.StyleEnum.FORM : explode,
                // allowReserved only applies to query parameters
                location == ParameterLocation.QUERY && Boolean.TRUE.equals(allowReserved));
    }

    /**
     * Serializes a path parameter value using RFC 6570 path expansion, returning the percent-encoded string
     * that replaces the parameter's template expression.
     */
    public String serializePathValue(String name, JsonNode value)
    {
        return switch (style) {
            case SIMPLE -> expand(name, value, "", ",", false);
            case LABEL -> expand(name, value, ".", ".", false);
            case MATRIX -> expand(name, value, ";", ";", true);
            default -> throw unsupportedStyle(name, ParameterLocation.PATH);
        };
    }

    /**
     * Serializes a query parameter value, returning percent-encoded {@code name=value} pairs.
     */
    public List<String> serializeQueryParameter(String name, JsonNode value)
    {
        String encodedName = UNRESERVED_ESCAPER.escape(name);
        if (style == Parameter.StyleEnum.DEEPOBJECT) {
            checkSupported(value.isObject(), "Parameter %s uses the deepObject style, which only supports object values", name);
            // brackets are delimiters, so they're not encoded, same as in the OpenAPI specification examples
            return fields(value).stream()
                    .map(field -> encodedName + "[" + UNRESERVED_ESCAPER.escape(field.getKey()) + "]=" + escapeQueryValue(field.getValue()))
                    .toList();
        }
        String delimiter = switch (style) {
            case FORM -> ",";
            case SPACEDELIMITED -> "%20";
            case PIPEDELIMITED -> "%7C";
            default -> throw unsupportedStyle(name, ParameterLocation.QUERY);
        };
        if (explode && value.isArray()) {
            return elements(value).stream()
                    .map(element -> encodedName + "=" + escapeQueryValue(element))
                    .toList();
        }
        if (explode && value.isObject()) {
            return fields(value).stream()
                    .map(field -> UNRESERVED_ESCAPER.escape(field.getKey()) + "=" + escapeQueryValue(field.getValue()))
                    .toList();
        }
        return List.of(flatten(value).stream()
                .map(this::escapeQueryValue)
                .collect(joining(delimiter, encodedName + "=", "")));
    }

    public String serializeHeaderValue(String name, JsonNode value)
    {
        if (style != Parameter.StyleEnum.SIMPLE) {
            throw unsupportedStyle(name, ParameterLocation.HEADER);
        }
        if (explode && value.isObject()) {
            return fields(value).stream()
                    .map(field -> field.getKey() + "=" + field.getValue())
                    .collect(joining(","));
        }
        return String.join(",", flatten(value));
    }

    /**
     * Serializes a cookie parameter value, returning a percent-encoded cookie value.
     */
    public String serializeCookieValue(String name, JsonNode value)
    {
        if (style != Parameter.StyleEnum.FORM) {
            throw unsupportedStyle(name, ParameterLocation.COOKIE);
        }
        // exploded objects would have to be sent as separate cookies, named after object properties
        checkSupported(!explode || !value.isObject(), "Parameter %s is an exploded object cookie, which is not supported", name);
        // arrays are always delimited with commas, because exploding them would require repeating the cookie name
        return UNRESERVED_ESCAPER.escape(String.join(",", flatten(value)));
    }

    private String expand(String name, JsonNode value, String prefix, String explodedSeparator, boolean named)
    {
        if (value.isContainerNode() && value.isEmpty()) {
            // RFC 6570 treats empty lists and associative arrays as undefined, and undefined values expand to an empty string
            return "";
        }
        String encodedName = UNRESERVED_ESCAPER.escape(name);
        if (explode && value.isArray()) {
            return elements(value).stream()
                    .map(element -> (named ? encodedName + "=" : "") + UNRESERVED_ESCAPER.escape(element))
                    .collect(joining(explodedSeparator, prefix, ""));
        }
        if (explode && value.isObject()) {
            return fields(value).stream()
                    .map(field -> UNRESERVED_ESCAPER.escape(field.getKey()) + "=" + UNRESERVED_ESCAPER.escape(field.getValue()))
                    .collect(joining(explodedSeparator, prefix, ""));
        }
        String values = flatten(value).stream()
                .map(UNRESERVED_ESCAPER::escape)
                .collect(joining(","));
        if (!named) {
            return prefix + values;
        }
        if (values.isEmpty()) {
            return prefix + encodedName;
        }
        return prefix + encodedName + "=" + values;
    }

    private String escapeQueryValue(String value)
    {
        if (!allowReserved) {
            return UNRESERVED_ESCAPER.escape(value);
        }
        // RFC 6570 reserved expansion also passes percent-encoded octets through unchanged
        StringBuilder result = new StringBuilder();
        Matcher matcher = PERCENT_ENCODED_OCTET.matcher(value);
        int start = 0;
        while (matcher.find()) {
            result.append(QUERY_RESERVED_ESCAPER.escape(value.substring(start, matcher.start())))
                    .append(matcher.group());
            start = matcher.end();
        }
        return result.append(QUERY_RESERVED_ESCAPER.escape(value.substring(start))).toString();
    }

    private TrinoException unsupportedStyle(String name, ParameterLocation location)
    {
        return new TrinoException(NOT_SUPPORTED, format("Parameter %s uses the %s style, which is not supported for %s parameters", name, style, location.name().toLowerCase(ENGLISH)));
    }

    private static void checkSupported(boolean condition, String message, String name)
    {
        if (!condition) {
            throw new TrinoException(NOT_SUPPORTED, format(message, name));
        }
    }

    /**
     * Returns array elements, object property names followed by their values, or a single primitive value.
     */
    private static List<String> flatten(JsonNode value)
    {
        if (!value.isObject()) {
            return elements(value);
        }
        return fields(value).stream()
                .flatMap(field -> Stream.of(field.getKey(), field.getValue()))
                .toList();
    }

    private static List<Map.Entry<String, String>> fields(JsonNode value)
    {
        return value.properties().stream()
                .map(field -> Map.entry(field.getKey(), asText(field.getValue())))
                .toList();
    }

    private static List<String> elements(JsonNode value)
    {
        if (!value.isArray()) {
            return List.of(asText(value));
        }
        return Streams.stream(value)
                .map(ParameterStyle::asText)
                .toList();
    }

    private static String asText(JsonNode value)
    {
        if (value.isBigDecimal()) {
            // BigDecimal.toString() uses the scientific notation for small values
            return value.decimalValue().toPlainString();
        }
        // nested values aren't defined by any style, so they're serialized as JSON
        return value.isValueNode() ? value.asText() : value.toString();
    }
}
