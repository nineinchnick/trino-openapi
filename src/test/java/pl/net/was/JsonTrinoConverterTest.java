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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class JsonTrinoConverterTest
{
    @Test
    void shouldConvertDifferentDateTimeFormatsIntoLong()
    {
        var type = TimestampType.createTimestampType(3);
        var schema = new Schema<>();

        schema.setFormat("date-time");

        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2024-09-24T19:17:58.638+04:00"), type, schema))
                .isEqualTo(1727191078638000L);

        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2024-09-24T18:30:47.94+04:00"), type, schema))
                .isEqualTo(1727188247940000L);
    }

    @Test
    void shouldConvertDateToLong()
    {
        var type = DateType.DATE;
        var schema = new Schema<>();

        schema.setFormat("date");

        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2024-09-24"), type, schema))
                .isEqualTo(19990);
    }

    @Test
    void shouldConvertMapUsingValueSchema()
    {
        var type = new MapType(VARCHAR, BIGINT, new TypeOperators());
        var schema = new MapSchema().additionalProperties(new IntegerSchema());
        MapBlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeString(keyBuilder, "answer");
            BIGINT.writeLong(valueBuilder, 42);
        });
        var objectMapper = new ObjectMapper();

        assertThat(JsonTrinoConverter.convert(blockBuilder.build(), 0, type, schema, objectMapper))
                .isEqualTo(objectMapper.createObjectNode().put("answer", 42L));
    }
}
