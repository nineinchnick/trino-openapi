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
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record ParameterStyle(Parameter.StyleEnum style, boolean explode)
{
    // RFC 3986 unreserved characters, which never have to be percent-encoded
    private static final Escaper UNRESERVED_ESCAPER = new PercentEscaper("-._~", false);

    public ParameterStyle
    {
        requireNonNull(style, "style is null");
    }

    public static ParameterStyle defaultFor(ParameterLocation location)
    {
        return switch (location) {
            case QUERY, COOKIE -> new ParameterStyle(Parameter.StyleEnum.FORM, true);
            default -> new ParameterStyle(Parameter.StyleEnum.SIMPLE, false);
        };
    }

    public static ParameterStyle of(Parameter.StyleEnum style, Boolean explode, ParameterLocation location)
    {
        Parameter.StyleEnum resolvedStyle = style == null ? defaultFor(location).style() : style;
        return new ParameterStyle(resolvedStyle, explode == null ? resolvedStyle == Parameter.StyleEnum.FORM : explode);
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
        String delimiter = switch (style) {
            case FORM -> ",";
            case SPACEDELIMITED -> "%20";
            case PIPEDELIMITED -> "%7C";
            default -> throw unsupportedStyle(name, ParameterLocation.QUERY);
        };
        String encodedName = UNRESERVED_ESCAPER.escape(name);
        List<String> values = elements(value).stream()
                .map(UNRESERVED_ESCAPER::escape)
                .toList();
        if (explode) {
            return values.stream()
                    .map(element -> encodedName + "=" + element)
                    .toList();
        }
        return List.of(encodedName + "=" + String.join(delimiter, values));
    }

    public String serializeHeaderValue(JsonNode value)
    {
        // header parameters only support the simple style, which delimits array elements with commas
        return String.join(",", elements(value));
    }

    public String serializeCookieValue(JsonNode value)
    {
        // cookie parameters only support the form style, which delimits array elements with commas
        return String.join(",", elements(value));
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

    private TrinoException unsupportedStyle(String name, ParameterLocation location)
    {
        return new TrinoException(NOT_SUPPORTED, format("Parameter %s uses the %s style, which is not supported for %s parameters", name, style, location.name().toLowerCase(ENGLISH)));
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
