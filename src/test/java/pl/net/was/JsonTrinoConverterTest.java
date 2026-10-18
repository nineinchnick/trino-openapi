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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
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

    @Test
    void shouldConvertRealToFloatBitsInRecords()
    {
        var schema = new NumberSchema().format("float");

        assertThat(JsonTrinoConverter.convertToRecordValue(DecimalNode.valueOf(new BigDecimal("1.5")), REAL, schema))
                .isEqualTo((long) floatToIntBits(1.5f));
        assertThat(JsonTrinoConverter.convertToRecordValue(DecimalNode.valueOf(new BigDecimal("-2.25")), REAL, schema))
                .isEqualTo((long) floatToIntBits(-2.25f));
        assertThat(JsonTrinoConverter.convertToRecordValue(DoubleNode.valueOf(Double.NaN), REAL, schema))
                .isEqualTo((long) floatToIntBits(Float.NaN));
        assertThat(JsonTrinoConverter.convertToRecordValue(DoubleNode.valueOf(Double.POSITIVE_INFINITY), REAL, schema))
                .isEqualTo((long) floatToIntBits(Float.POSITIVE_INFINITY));
        assertThat(JsonTrinoConverter.convertToRecordValue(DoubleNode.valueOf(Double.NEGATIVE_INFINITY), REAL, schema))
                .isEqualTo((long) floatToIntBits(Float.NEGATIVE_INFINITY));
        assertThat(JsonTrinoConverter.convertToRecordValue(NullNode.getInstance(), REAL, schema))
                .isNull();
        assertThat(JsonTrinoConverter.convertToRecordValue(null, REAL, schema))
                .isNull();
    }

    @Test
    void shouldReadRealRecordValuesWithInMemoryRecordSet()
    {
        var schema = new NumberSchema().format("float");
        List<Object> record = Arrays.asList(
                JsonTrinoConverter.convertToRecordValue(DecimalNode.valueOf(new BigDecimal("1.5")), REAL, schema),
                JsonTrinoConverter.convertToRecordValue(DecimalNode.valueOf(new BigDecimal("-2.25")), REAL, schema),
                JsonTrinoConverter.convertToRecordValue(DoubleNode.valueOf(Double.NaN), REAL, schema),
                JsonTrinoConverter.convertToRecordValue(DoubleNode.valueOf(Double.NEGATIVE_INFINITY), REAL, schema),
                JsonTrinoConverter.convertToRecordValue(NullNode.getInstance(), REAL, schema));
        RecordCursor cursor = new InMemoryRecordSet(Collections.nCopies(record.size(), REAL), List.of(record)).cursor();

        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(intBitsToFloat(toIntExact(cursor.getLong(0)))).isEqualTo(1.5f);
        assertThat(intBitsToFloat(toIntExact(cursor.getLong(1)))).isEqualTo(-2.25f);
        assertThat(intBitsToFloat(toIntExact(cursor.getLong(2)))).isNaN();
        assertThat(intBitsToFloat(toIntExact(cursor.getLong(3)))).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(cursor.isNull(4)).isTrue();
    }

    @Test
    void shouldKeepNestedRealValues()
    {
        var type = new ArrayType(REAL);
        var schema = new ArraySchema().items(new NumberSchema().format("float"));
        var objectMapper = new ObjectMapper();
        ArrayNode values = objectMapper.createArrayNode()
                .add(new BigDecimal("1.5"))
                .add(new BigDecimal("-2.25"))
                .add(DoubleNode.valueOf(Double.POSITIVE_INFINITY))
                .addNull();

        Block block = (Block) JsonTrinoConverter.convert(values, type, schema);

        assertThat(REAL.getFloat(block, 0)).isEqualTo(1.5f);
        assertThat(REAL.getFloat(block, 1)).isEqualTo(-2.25f);
        assertThat(REAL.getFloat(block, 2)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(block.isNull(3)).isTrue();
    }
}
