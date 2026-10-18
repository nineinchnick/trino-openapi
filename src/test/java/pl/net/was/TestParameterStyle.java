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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestParameterStyle
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String RESERVED = "a/b?c&d#e f";
    private static final JsonNode PRIMITIVE = TextNode.valueOf("blue");
    private static final JsonNode ARRAY = OBJECT_MAPPER.createArrayNode().add("blue").add("black").add("brown");
    private static final JsonNode OBJECT = OBJECT_MAPPER.createObjectNode().put("R", 100).put("G", 200).put("B", 150);

    @Test
    void testSimplePathStyle()
    {
        assertThat(pathValue(StyleEnum.SIMPLE, false, PRIMITIVE)).isEqualTo("blue");
        assertThat(pathValue(StyleEnum.SIMPLE, true, PRIMITIVE)).isEqualTo("blue");
        assertThat(pathValue(StyleEnum.SIMPLE, false, ARRAY)).isEqualTo("blue,black,brown");
        assertThat(pathValue(StyleEnum.SIMPLE, true, ARRAY)).isEqualTo("blue,black,brown");
        assertThat(pathValue(StyleEnum.SIMPLE, false, OBJECT)).isEqualTo("R,100,G,200,B,150");
        assertThat(pathValue(StyleEnum.SIMPLE, true, OBJECT)).isEqualTo("R=100,G=200,B=150");
    }

    @Test
    void testLabelPathStyle()
    {
        assertThat(pathValue(StyleEnum.LABEL, false, PRIMITIVE)).isEqualTo(".blue");
        assertThat(pathValue(StyleEnum.LABEL, true, PRIMITIVE)).isEqualTo(".blue");
        assertThat(pathValue(StyleEnum.LABEL, false, ARRAY)).isEqualTo(".blue,black,brown");
        assertThat(pathValue(StyleEnum.LABEL, true, ARRAY)).isEqualTo(".blue.black.brown");
        assertThat(pathValue(StyleEnum.LABEL, false, OBJECT)).isEqualTo(".R,100,G,200,B,150");
        assertThat(pathValue(StyleEnum.LABEL, true, OBJECT)).isEqualTo(".R=100.G=200.B=150");
    }

    @Test
    void testMatrixPathStyle()
    {
        assertThat(pathValue(StyleEnum.MATRIX, false, PRIMITIVE)).isEqualTo(";color=blue");
        assertThat(pathValue(StyleEnum.MATRIX, true, PRIMITIVE)).isEqualTo(";color=blue");
        assertThat(pathValue(StyleEnum.MATRIX, false, TextNode.valueOf(""))).isEqualTo(";color");
        assertThat(pathValue(StyleEnum.MATRIX, false, ARRAY)).isEqualTo(";color=blue,black,brown");
        assertThat(pathValue(StyleEnum.MATRIX, true, ARRAY)).isEqualTo(";color=blue;color=black;color=brown");
        assertThat(pathValue(StyleEnum.MATRIX, false, OBJECT)).isEqualTo(";color=R,100,G,200,B,150");
        assertThat(pathValue(StyleEnum.MATRIX, true, OBJECT)).isEqualTo(";R=100;G=200;B=150");
    }

    @Test
    void testEmptyPathValues()
    {
        for (StyleEnum style : new StyleEnum[] {StyleEnum.SIMPLE, StyleEnum.LABEL, StyleEnum.MATRIX}) {
            assertThat(pathValue(style, false, OBJECT_MAPPER.createArrayNode())).isEmpty();
            assertThat(pathValue(style, true, OBJECT_MAPPER.createObjectNode())).isEmpty();
        }
    }

    @Test
    void testPathValuesWithReservedCharacters()
    {
        ArrayNode array = OBJECT_MAPPER.createArrayNode().add(RESERVED).add("x,y");
        ObjectNode object = OBJECT_MAPPER.createObjectNode().put("k=1", RESERVED);
        assertThat(pathValue(StyleEnum.SIMPLE, false, TextNode.valueOf(RESERVED))).isEqualTo("a%2Fb%3Fc%26d%23e%20f");
        assertThat(pathValue(StyleEnum.SIMPLE, false, TextNode.valueOf("{id};.~-_%zażółć"))).isEqualTo("%7Bid%7D%3B.~-_%25za%C5%BC%C3%B3%C5%82%C4%87");
        assertThat(pathValue(StyleEnum.SIMPLE, false, array)).isEqualTo("a%2Fb%3Fc%26d%23e%20f,x%2Cy");
        assertThat(pathValue(StyleEnum.SIMPLE, true, object)).isEqualTo("k%3D1=a%2Fb%3Fc%26d%23e%20f");
        assertThat(pathValue(StyleEnum.LABEL, true, array)).isEqualTo(".a%2Fb%3Fc%26d%23e%20f.x%2Cy");
        assertThat(new ParameterStyle(StyleEnum.MATRIX, true).serializePathValue("a;b", array))
                .isEqualTo(";a%3Bb=a%2Fb%3Fc%26d%23e%20f;a%3Bb=x%2Cy");
    }

    @Test
    void testDecimalValuesInPlainNotation()
    {
        JsonNode decimals = OBJECT_MAPPER.createArrayNode().add(new BigDecimal("0.00000001")).add(new BigDecimal("1E+3"));
        assertThat(pathValue(StyleEnum.SIMPLE, false, decimals)).isEqualTo("0.00000001,1000");
        assertThat(queryParameter(StyleEnum.FORM, true, decimals)).containsExactly("color=0.00000001", "color=1000");
        assertThat(pathValue(StyleEnum.SIMPLE, false, DecimalNode.valueOf(new BigDecimal("-1E-8")))).isEqualTo("-0.00000001");
    }

    @Test
    void testUnsupportedPathStyles()
    {
        for (StyleEnum style : new StyleEnum[] {StyleEnum.FORM, StyleEnum.SPACEDELIMITED, StyleEnum.PIPEDELIMITED, StyleEnum.DEEPOBJECT}) {
            assertThatThrownBy(() -> pathValue(style, false, PRIMITIVE))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                    .hasMessage("Parameter color uses the %s style, which is not supported for path parameters", style);
        }
    }

    @Test
    void testFormQueryStyle()
    {
        assertThat(queryParameter(StyleEnum.FORM, true, PRIMITIVE)).containsExactly("color=blue");
        assertThat(queryParameter(StyleEnum.FORM, false, PRIMITIVE)).containsExactly("color=blue");
        assertThat(queryParameter(StyleEnum.FORM, true, ARRAY)).containsExactly("color=blue", "color=black", "color=brown");
        assertThat(queryParameter(StyleEnum.FORM, false, ARRAY)).containsExactly("color=blue,black,brown");
    }

    @Test
    void testFormQueryStyleObjects()
    {
        assertThat(queryParameter(StyleEnum.FORM, true, OBJECT)).containsExactly("R=100", "G=200", "B=150");
        assertThat(queryParameter(StyleEnum.FORM, false, OBJECT)).containsExactly("color=R,100,G,200,B,150");
        assertThat(queryParameter(StyleEnum.FORM, true, OBJECT_MAPPER.createObjectNode())).isEmpty();
    }

    @Test
    void testDelimitedQueryStyles()
    {
        assertThat(queryParameter(StyleEnum.SPACEDELIMITED, false, ARRAY)).containsExactly("color=blue%20black%20brown");
        assertThat(queryParameter(StyleEnum.PIPEDELIMITED, false, ARRAY)).containsExactly("color=blue%7Cblack%7Cbrown");
        assertThat(queryParameter(StyleEnum.PIPEDELIMITED, true, ARRAY)).containsExactly("color=blue", "color=black", "color=brown");
        assertThat(queryParameter(StyleEnum.SPACEDELIMITED, false, OBJECT)).containsExactly("color=R%20100%20G%20200%20B%20150");
        assertThat(queryParameter(StyleEnum.PIPEDELIMITED, false, OBJECT)).containsExactly("color=R%7C100%7CG%7C200%7CB%7C150");
    }

    @Test
    void testDeepObjectQueryStyle()
    {
        assertThat(queryParameter(StyleEnum.DEEPOBJECT, true, OBJECT)).containsExactly("color[R]=100", "color[G]=200", "color[B]=150");
        assertThat(queryParameter(StyleEnum.DEEPOBJECT, true, OBJECT_MAPPER.createObjectNode().put("a[b]", RESERVED)))
                .containsExactly("color[a%5Bb%5D]=a%2Fb%3Fc%26d%23e%20f");
        assertThat(queryParameter(StyleEnum.DEEPOBJECT, true, OBJECT_MAPPER.createObjectNode().set("nested", ARRAY)))
                .containsExactly("color[nested]=%5B%22blue%22%2C%22black%22%2C%22brown%22%5D");
        for (JsonNode value : new JsonNode[] {PRIMITIVE, ARRAY}) {
            assertThatThrownBy(() -> queryParameter(StyleEnum.DEEPOBJECT, true, value))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                    .hasMessage("Parameter color uses the deepObject style, which only supports object values");
        }
    }

    @Test
    void testQueryValuesWithReservedCharacters()
    {
        assertThat(queryParameter(StyleEnum.FORM, true, TextNode.valueOf(RESERVED + "+=%"))).containsExactly("color=a%2Fb%3Fc%26d%23e%20f%2B%3D%25");
        assertThat(queryParameter(StyleEnum.FORM, false, OBJECT_MAPPER.createArrayNode().add("a,b").add("c|d")))
                .containsExactly("color=a%2Cb,c%7Cd");
        assertThat(new ParameterStyle(StyleEnum.FORM, true).serializeQueryParameter("a&b=", PRIMITIVE)).containsExactly("a%26b%3D=blue");
    }

    @Test
    void testQueryValuesAllowingReservedCharacters()
    {
        ParameterStyle form = new ParameterStyle(StyleEnum.FORM, true, true);
        assertThat(form.serializeQueryParameter("a/b", TextNode.valueOf(RESERVED + "+=:@!$'()*,;[]%2F%zz")))
                .containsExactly("a%2Fb=a/b?c&d%23e%20f+=:@!$'()*,;%5B%5D%2F%25zz");
        assertThat(form.serializeQueryParameter("color", OBJECT_MAPPER.createObjectNode().put("a/b", "c/d")))
                .containsExactly("a%2Fb=c/d");
        assertThat(new ParameterStyle(StyleEnum.PIPEDELIMITED, false, true).serializeQueryParameter("color", OBJECT_MAPPER.createArrayNode().add("a/b").add("c d")))
                .containsExactly("color=a/b%7Cc%20d");
        assertThat(new ParameterStyle(StyleEnum.DEEPOBJECT, true, true).serializeQueryParameter("color", OBJECT_MAPPER.createObjectNode().put("a/b", "c/d")))
                .containsExactly("color[a%2Fb]=c/d");
    }

    @Test
    void testAllowReservedOnlyAppliesToQueryParameters()
    {
        assertThat(ParameterStyle.of(null, null, true, ParameterLocation.QUERY)).isEqualTo(new ParameterStyle(StyleEnum.FORM, true, true));
        assertThat(ParameterStyle.of(null, null, true, ParameterLocation.PATH)).isEqualTo(ParameterStyle.defaultFor(ParameterLocation.PATH));
        assertThat(ParameterStyle.of(null, null, true, ParameterLocation.HEADER)).isEqualTo(ParameterStyle.defaultFor(ParameterLocation.HEADER));
        assertThat(ParameterStyle.of(null, null, true, ParameterLocation.COOKIE)).isEqualTo(ParameterStyle.defaultFor(ParameterLocation.COOKIE));
    }

    @Test
    void testUnsupportedQueryStyles()
    {
        for (StyleEnum style : new StyleEnum[] {StyleEnum.SIMPLE, StyleEnum.LABEL, StyleEnum.MATRIX}) {
            assertThatThrownBy(() -> queryParameter(style, false, PRIMITIVE))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                    .hasMessage("Parameter color uses the %s style, which is not supported for query parameters", style);
        }
    }

    @Test
    void testHeaderValues()
    {
        assertThat(headerValue(false, PRIMITIVE)).isEqualTo("blue");
        assertThat(headerValue(false, TextNode.valueOf(RESERVED))).isEqualTo(RESERVED);
        assertThat(headerValue(false, ARRAY)).isEqualTo("blue,black,brown");
        assertThat(headerValue(true, ARRAY)).isEqualTo("blue,black,brown");
        assertThat(headerValue(false, OBJECT)).isEqualTo("R,100,G,200,B,150");
        assertThat(headerValue(true, OBJECT)).isEqualTo("R=100,G=200,B=150");
        assertThatThrownBy(() -> new ParameterStyle(StyleEnum.FORM, false).serializeHeaderValue("color", PRIMITIVE))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Parameter color uses the form style, which is not supported for header parameters");
    }

    @Test
    void testCookieValues()
    {
        assertThat(cookieValue(true, PRIMITIVE)).isEqualTo("blue");
        assertThat(cookieValue(true, TextNode.valueOf(RESERVED + ";,"))).isEqualTo("a%2Fb%3Fc%26d%23e%20f%3B%2C");
        assertThat(cookieValue(true, ARRAY)).isEqualTo("blue%2Cblack%2Cbrown");
        assertThat(cookieValue(false, OBJECT)).isEqualTo("R%2C100%2CG%2C200%2CB%2C150");
        assertThatThrownBy(() -> cookieValue(true, OBJECT))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Parameter color is an exploded object cookie, which is not supported");
        assertThatThrownBy(() -> new ParameterStyle(StyleEnum.SIMPLE, false).serializeCookieValue("color", PRIMITIVE))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Parameter color uses the simple style, which is not supported for cookie parameters");
    }

    private static String headerValue(boolean explode, JsonNode value)
    {
        return new ParameterStyle(StyleEnum.SIMPLE, explode).serializeHeaderValue("color", value);
    }

    private static String cookieValue(boolean explode, JsonNode value)
    {
        return new ParameterStyle(StyleEnum.FORM, explode).serializeCookieValue("color", value);
    }

    private static String pathValue(StyleEnum style, boolean explode, JsonNode value)
    {
        return new ParameterStyle(style, explode).serializePathValue("color", value);
    }

    private static List<String> queryParameter(StyleEnum style, boolean explode, JsonNode value)
    {
        return new ParameterStyle(style, explode).serializeQueryParameter("color", value);
    }
}
