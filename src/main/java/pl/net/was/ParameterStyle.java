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
import io.swagger.v3.oas.models.parameters.Parameter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record ParameterStyle(Parameter.StyleEnum style, boolean explode)
{
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

    public List<String> serializeQueryValues(JsonNode value)
    {
        List<String> values = elements(value);
        if (explode) {
            return values;
        }
        return List.of(String.join(arrayDelimiter(), values));
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

    private String arrayDelimiter()
    {
        return switch (style) {
            case SPACEDELIMITED -> " ";
            case PIPEDELIMITED -> "|";
            default -> ",";
        };
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
