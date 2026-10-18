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

import io.swagger.v3.oas.models.parameters.Parameter;

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

    public String arrayDelimiter()
    {
        return switch (style) {
            case SPACEDELIMITED -> " ";
            case PIPEDELIMITED -> "|";
            default -> ",";
        };
    }
}
