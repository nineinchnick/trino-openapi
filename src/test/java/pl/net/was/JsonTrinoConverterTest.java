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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
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

    @Test
    void shouldConvertRealToJsonNumbers()
    {
        var type = new ArrayType(REAL);
        var schema = new ArraySchema().items(new NumberSchema().format("float"));
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, 5);
        REAL.writeFloat(blockBuilder, 1.5f);
        REAL.writeFloat(blockBuilder, -2.25f);
        REAL.writeFloat(blockBuilder, Float.NaN);
        REAL.writeFloat(blockBuilder, Float.POSITIVE_INFINITY);
        blockBuilder.appendNull();
        Block block = blockBuilder.build();
        var objectMapper = new ObjectMapper();

        assertThat(JsonTrinoConverter.convert(block, 0, REAL, schema.getItems(), objectMapper)).isEqualTo(1.5f);
        assertThat(JsonTrinoConverter.convert(block, 1, REAL, schema.getItems(), objectMapper)).isEqualTo(-2.25f);
        assertThat(JsonTrinoConverter.convert(block, 2, REAL, schema.getItems(), objectMapper)).isEqualTo(Float.NaN);
        assertThat(JsonTrinoConverter.convert(block, 3, REAL, schema.getItems(), objectMapper)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(JsonTrinoConverter.convert(block, 4, REAL, schema.getItems(), objectMapper)).isNull();
        assertThat(JsonTrinoConverter.convert(block, 0, type, schema, objectMapper))
                .isEqualTo(objectMapper.createArrayNode()
                        .add(1.5f)
                        .add(-2.25f)
                        .add(Float.NaN)
                        .add(Float.POSITIVE_INFINITY)
                        .addNull());
    }

    @Test
    void shouldConvertDecimalsToJsonNumbers()
    {
        var shortType = createDecimalType(18, 8);
        var longType = createDecimalType(38, 10);
        var shortValue = new BigDecimal("-1234567890.12345678");
        var longValue = new BigDecimal("9999999999999999999999999999.9999999999");
        BlockBuilder shortBlockBuilder = shortType.createBlockBuilder(null, 2);
        shortType.writeLong(shortBlockBuilder, shortValue.unscaledValue().longValueExact());
        shortBlockBuilder.appendNull();
        BlockBuilder longBlockBuilder = longType.createBlockBuilder(null, 1);
        longType.writeObject(longBlockBuilder, Int128.valueOf(longValue.unscaledValue()));
        var objectMapper = new ObjectMapper();

        assertThat(JsonTrinoConverter.convert(shortBlockBuilder.build(), 0, shortType, new NumberSchema(), objectMapper)).isEqualTo(shortValue);
        assertThat(JsonTrinoConverter.convert(shortBlockBuilder.build(), 1, shortType, new NumberSchema(), objectMapper)).isNull();
        assertThat(JsonTrinoConverter.convert(longBlockBuilder.build(), 0, longType, new NumberSchema(), objectMapper)).isEqualTo(longValue);
    }

    @Test
    void shouldConvertNestedRealAndDecimalsToJsonNumbers()
            throws JsonProcessingException
    {
        var decimalType = createDecimalType(18, 8);
        var rowType = RowType.from(List.of(
                RowType.field("real_value", REAL),
                RowType.field("decimal_value", decimalType),
                RowType.field("missing", decimalType)));
        var rowSchema = new ObjectSchema()
                .addProperty("real_value", new NumberSchema().format("float"))
                .addProperty("decimal_value", new NumberSchema())
                .addProperty("missing", new NumberSchema());
        var arrayType = new ArrayType(decimalType);
        var mapType = new MapType(VARCHAR, REAL, new TypeOperators());
        var decimalValue = new BigDecimal("1234567890.12345678");

        RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            REAL.writeFloat(fieldBuilders.get(0), -0.5f);
            decimalType.writeLong(fieldBuilders.get(1), decimalValue.unscaledValue().longValueExact());
            fieldBuilders.get(2).appendNull();
        });
        ArrayBlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, 1);
        arrayBlockBuilder.buildEntry(elementBuilder -> {
            decimalType.writeLong(elementBuilder, decimalValue.unscaledValue().longValueExact());
            decimalType.writeLong(elementBuilder, 1);
            elementBuilder.appendNull();
        });
        MapBlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeString(keyBuilder, "real");
            REAL.writeFloat(valueBuilder, 2.5f);
            VARCHAR.writeString(keyBuilder, "missing");
            valueBuilder.appendNull();
        });
        var objectMapper = JsonMapper.builder()
                .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
                .build();

        Object row = JsonTrinoConverter.convert(rowBlockBuilder.build(), 0, rowType, rowSchema, objectMapper);
        Object array = JsonTrinoConverter.convert(arrayBlockBuilder.build(), 0, arrayType, new ArraySchema().items(new NumberSchema()), objectMapper);
        Object map = JsonTrinoConverter.convert(mapBlockBuilder.build(), 0, mapType, new MapSchema().additionalProperties(new NumberSchema().format("float")), objectMapper);

        assertThat(objectMapper.writeValueAsString(row)).isEqualTo("{\"real_value\":-0.5,\"decimal_value\":1234567890.12345678,\"missing\":null}");
        assertThat(objectMapper.writeValueAsString(array)).isEqualTo("[1234567890.12345678,0.00000001,null]");
        assertThat(objectMapper.writeValueAsString(map)).isEqualTo("{\"real\":2.5,\"missing\":null}");
    }
}
