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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.trino.spi.TrinoException;
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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;

class JsonTrinoConverterTest
{
    // 2025-04-18T12:30:45Z
    private static final long EPOCH_MICROS = 1_744_979_445_000_000L;
    private static final int EPOCH_DAY = 20_196;

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
    void shouldParseDateTimeWithPrecisionOfColumn()
    {
        var schema = new Schema<>().format("date-time");
        var text = TextNode.valueOf("2025-04-18T12:30:45.123456789Z");

        assertThat(JsonTrinoConverter.convert(text, TimestampType.createTimestampType(0), schema))
                .isEqualTo(EPOCH_MICROS);
        assertThat(JsonTrinoConverter.convert(text, TimestampType.createTimestampType(3), schema))
                .isEqualTo(EPOCH_MICROS + 123_000);
        assertThat(JsonTrinoConverter.convert(text, TimestampType.createTimestampType(6), schema))
                .isEqualTo(EPOCH_MICROS + 123_457);
        assertThat(JsonTrinoConverter.convert(text, TimestampType.createTimestampType(9), schema))
                .isEqualTo(new LongTimestamp(EPOCH_MICROS + 123_456, 789_000));
    }

    @Test
    void shouldRoundParsedDateTimeToPrecisionOfColumn()
    {
        var schema = new Schema<>().format("date-time");

        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2025-04-18T12:30:45.5Z"), TimestampType.createTimestampType(0), schema))
                .isEqualTo(EPOCH_MICROS + 1_000_000);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2025-04-18T12:30:44.9995Z"), TimestampType.createTimestampType(3), schema))
                .isEqualTo(EPOCH_MICROS);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2025-04-18T12:30:44.9999995Z"), TimestampType.createTimestampType(6), schema))
                .isEqualTo(EPOCH_MICROS);
    }

    @Test
    void shouldParseDateTimeWithOffsetsAsUtc()
    {
        var type = TimestampType.createTimestampType(3);
        var schema = new Schema<>().format("date-time");

        assertThat(List.of(
                "2025-04-18T12:30:45.250Z",
                "2025-04-18T12:30:45.25+00:00",
                "2025-04-18T14:30:45.25+02:00",
                "2025-04-18T07:00:45.25-05:30",
                "2025-04-18T14:30:45.25+02:00[Europe/Warsaw]",
                // without an offset, the value is already in UTC
                "2025-04-18T12:30:45.25"))
                .allSatisfy(text -> assertThat(JsonTrinoConverter.convert(TextNode.valueOf(text), type, schema))
                        .as(text)
                        .isEqualTo(EPOCH_MICROS + 250_000));
    }

    @Test
    void shouldParseDateTimeWithCustomFormats()
    {
        var type = TimestampType.createTimestampType(3);
        Locale defaultLocale = Locale.getDefault();
        try {
            // month names in custom formats must not depend on the default locale
            Locale.setDefault(Locale.GERMANY);
            assertThat(JsonTrinoConverter.convert(TextNode.valueOf("2025/04/18 12:30:45"), type, new Schema<>().format("yyyy/MM/dd HH:mm:ss")))
                    .isEqualTo(EPOCH_MICROS);
            assertThat(JsonTrinoConverter.convert(TextNode.valueOf("18 Apr 2025 14:30:45.123+02:00"), type, new Schema<>().format("dd MMM yyyy HH:mm:ss.SSSXXX")))
                    .isEqualTo(EPOCH_MICROS + 123_000);
            assertThat(JsonTrinoConverter.convert(TextNode.valueOf("Fri, 18 Apr 2025 12:30:45 GMT"), type, new Schema<>().format("EEE, dd MMM yyyy HH:mm:ss zzz")))
                    .isEqualTo(EPOCH_MICROS);
            assertThat(JsonTrinoConverter.convert(TextNode.valueOf("20250418"), type, new Schema<>().format("yyyyMMdd")))
                    .isEqualTo(EPOCH_MICROS - (12 * 3600 + 30 * 60 + 45) * 1_000_000L);
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    void shouldParseNestedLongTimestamps()
    {
        var type = new ArrayType(TimestampType.TIMESTAMP_NANOS);
        var schema = new ArraySchema().items(new Schema<>().format("date-time"));
        ArrayNode values = new ObjectMapper().createArrayNode()
                .add("2025-04-18T12:30:45.123456789Z")
                .addNull();

        Block block = (Block) JsonTrinoConverter.convert(values, type, schema);

        assertThat(TimestampType.TIMESTAMP_NANOS.getObject(block, 0)).isEqualTo(new LongTimestamp(EPOCH_MICROS + 123_456, 789_000));
        assertThat(block.isNull(1)).isTrue();
    }

    @Test
    void shouldRejectInvalidDateTimes()
    {
        var type = TimestampType.createTimestampType(3);

        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("2025-04-18 12:30:45"), type, new Schema<>().format("date-time")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as timestamp(3)")
                .hasNoCause();
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("18.04.2025"), DateType.DATE, new Schema<>().format("date")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as date")
                .hasNoCause();
    }

    @Test
    void shouldFormatDateTimeWithPrecisionOfColumn()
    {
        var schema = new Schema<>().format("date-time");

        assertThat(formatTimestamp(TimestampType.createTimestampType(0), EPOCH_MICROS, schema))
                .isEqualTo("2025-04-18T12:30:45Z");
        assertThat(formatTimestamp(TimestampType.createTimestampType(3), EPOCH_MICROS + 120_000, schema))
                .isEqualTo("2025-04-18T12:30:45.120Z");
        assertThat(formatTimestamp(TimestampType.createTimestampType(6), EPOCH_MICROS + 123_456, schema))
                .isEqualTo("2025-04-18T12:30:45.123456Z");
        assertThat(formatTimestamp(TimestampType.TIMESTAMP_NANOS, new LongTimestamp(EPOCH_MICROS + 123_456, 789_000), schema))
                .isEqualTo("2025-04-18T12:30:45.123456789Z");
        assertThat(formatTimestamp(TimestampType.createTimestampType(3), -1_000L, schema))
                .isEqualTo("1969-12-31T23:59:59.999Z");
        assertThat(formatTimestamp(TimestampType.createTimestampType(3), EPOCH_MICROS, new Schema<>()))
                .isEqualTo("2025-04-18T12:30:45.000Z");
    }

    @Test
    void shouldFormatDateTimeWithCustomFormats()
    {
        var type = TimestampType.createTimestampType(3);
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            assertThat(formatTimestamp(type, EPOCH_MICROS + 123_000, new Schema<>().format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))
                    .isEqualTo("2025-04-18T12:30:45.123Z");
            assertThat(formatTimestamp(type, EPOCH_MICROS, new Schema<>().format("yyyy-MM-dd HH:mm:ss xxx")))
                    .isEqualTo("2025-04-18 12:30:45 +00:00");
            assertThat(formatTimestamp(type, EPOCH_MICROS, new Schema<>().format("EEE, dd MMM yyyy HH:mm:ss 'GMT'")))
                    .isEqualTo("Fri, 18 Apr 2025 12:30:45 GMT");
            assertThat(formatTimestamp(type, EPOCH_MICROS, new Schema<>().format("yyyyMMdd")))
                    .isEqualTo("20250418");
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    void shouldConvertDatesWithStandardAndCustomFormats()
    {
        var objectMapper = new ObjectMapper();
        BlockBuilder blockBuilder = DateType.DATE.createBlockBuilder(null, 1);
        DateType.DATE.writeLong(blockBuilder, EPOCH_DAY);
        Block block = blockBuilder.build();

        assertThat(JsonTrinoConverter.convert(block, 0, DateType.DATE, new Schema<>().format("date"), objectMapper))
                .isEqualTo("2025-04-18");
        assertThat(JsonTrinoConverter.convert(block, 0, DateType.DATE, new Schema<>(), objectMapper))
                .isEqualTo("2025-04-18");
        assertThat(JsonTrinoConverter.convert(block, 0, DateType.DATE, new Schema<>().format("dd.MM.yyyy"), objectMapper))
                .isEqualTo("18.04.2025");
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("18.04.2025"), DateType.DATE, new Schema<>().format("dd.MM.yyyy")))
                .isEqualTo(EPOCH_DAY);
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

    @Test
    void shouldConvertUnionFieldsInRowsToRawJson()
            throws JsonProcessingException
    {
        var rowType = RowType.from(List.of(
                RowType.field("name", VARCHAR),
                RowType.field("value", VARCHAR)));
        var rowSchema = new ObjectSchema()
                .addProperty("name", new StringSchema())
                .addProperty("value", new Schema<>().oneOf(List.of(new IntegerSchema(), new ObjectSchema())));
        RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            VARCHAR.writeString(fieldBuilders.get(0), "answer");
            VARCHAR.writeString(fieldBuilders.get(1), "{\"number\":42}");
        });
        var objectMapper = new ObjectMapper();

        Object row = JsonTrinoConverter.convert(rowBlockBuilder.build(), 0, rowType, rowSchema, objectMapper);

        assertThat(objectMapper.writeValueAsString(row)).isEqualTo("{\"name\":\"answer\",\"value\":{\"number\":42}}");
    }

    @Test
    void shouldConvertDecimalsWithMaximumPrecision()
    {
        var shortType = createDecimalType(18, 8);
        var longType = createDecimalType(38, 10);
        var schema = new NumberSchema();

        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("9999999999.99999999")), shortType, schema))
                .isEqualTo(999999999999999999L);
        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("-1234567890.12345678")), shortType, schema))
                .isEqualTo(-123456789012345678L);
        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("0.00000001")), shortType, schema))
                .isEqualTo(1L);
        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("12")), shortType, schema))
                .isEqualTo(1200000000L);
        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("0.000000005")), shortType, schema))
                .isEqualTo(1L);
        assertThat(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("9999999999999999999999999999.9999999999")), longType, schema))
                .isEqualTo(Int128.valueOf("99999999999999999999999999999999999999"));
        assertThat(JsonTrinoConverter.convert(NullNode.getInstance(), shortType, schema))
                .isNull();
    }

    @Test
    void shouldParseDecimalsFromExactJsonText()
            throws JsonProcessingException
    {
        var type = createDecimalType(18, 8);
        var objectMapper = JsonMapper.builder()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .build();

        JsonNode node = objectMapper.readTree("{\"value\": 1234567890.12345678}");

        assertThat(JsonTrinoConverter.convert(node.get("value"), type, new NumberSchema()))
                .isEqualTo(123456789012345678L);
    }

    @Test
    void shouldRejectDecimalsExceedingPrecision()
    {
        var type = createDecimalType(18, 8);

        assertThatThrownBy(() -> JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("10000000000")), type, new NumberSchema()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Value doesn't fit in decimal(18,8)");
    }

    @Test
    void shouldConvertDecimalsFromJsonStrings()
    {
        var shortType = createDecimalType(18, 8);
        var longType = createDecimalType(38, 10);
        var schema = new NumberSchema();

        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("12.50"), shortType, schema))
                .isEqualTo(JsonTrinoConverter.convert(DecimalNode.valueOf(new BigDecimal("12.50")), shortType, schema))
                .isEqualTo(1250000000L);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("-1234567890.12345678"), shortType, schema))
                .isEqualTo(-123456789012345678L);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("0.000000005"), shortType, schema))
                .isEqualTo(1L);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("1E-8"), shortType, schema))
                .isEqualTo(1L);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("9999999999999999999999999999.9999999999"), longType, schema))
                .isEqualTo(Int128.valueOf("99999999999999999999999999999999999999"));
    }

    @Test
    void shouldRejectInvalidDecimalStrings()
    {
        var type = createDecimalType(18, 8);

        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("abc"), type, new NumberSchema()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot read a JSON string as decimal(18,8)");
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf(""), type, new NumberSchema()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot read a JSON string as decimal(18,8)");
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("10000000000"), type, new NumberSchema()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Value doesn't fit in decimal(18,8)");
    }

    @Test
    void shouldConvertOtherNumbersFromJsonStrings()
    {
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("42"), INTEGER, new IntegerSchema()))
                .isEqualTo(JsonTrinoConverter.convert(IntNode.valueOf(42), INTEGER, new IntegerSchema()))
                .isEqualTo(42);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("9223372036854775807"), BIGINT, new IntegerSchema()))
                .isEqualTo(JsonTrinoConverter.convert(LongNode.valueOf(Long.MAX_VALUE), BIGINT, new IntegerSchema()))
                .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE));
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("-2.25"), REAL, new NumberSchema().format("float")))
                .isEqualTo(JsonTrinoConverter.convert(DoubleNode.valueOf(-2.25), REAL, new NumberSchema().format("float")))
                .isEqualTo(-2.25f);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("1.5"), DOUBLE, new NumberSchema().format("double")))
                .isEqualTo(JsonTrinoConverter.convert(DoubleNode.valueOf(1.5), DOUBLE, new NumberSchema().format("double")))
                .isEqualTo(1.5);
        assertThat(JsonTrinoConverter.convert(TextNode.valueOf("NaN"), DOUBLE, new NumberSchema().format("double")))
                .isEqualTo(Double.NaN);
    }

    @Test
    void shouldRejectNonArraysForArrayColumns()
    {
        var type = new ArrayType(VARCHAR);

        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("secret"), type, new ArraySchema().items(new StringSchema())))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON STRING as array(varchar)");
    }

    @Test
    void shouldRejectInvalidNumberStrings()
    {
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("abc"), INTEGER, new IntegerSchema()))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as integer")
                .hasNoCause();
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("1.5"), BIGINT, new IntegerSchema()))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as bigint")
                .hasNoCause();
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("abc"), REAL, new NumberSchema().format("float")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as real")
                .hasNoCause();
        assertThatThrownBy(() -> JsonTrinoConverter.convert(TextNode.valueOf("abc"), DOUBLE, new NumberSchema().format("double")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Cannot read a JSON string as double")
                .hasNoCause();
    }

    @Test
    void shouldFormatDecimalsIndependentlyOfLocale()
    {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);

            assertThat(JsonTrinoConverter.toBigDecimal(123456789012345678L, createDecimalType(18, 8)).toPlainString())
                    .isEqualTo("1234567890.12345678");
            assertThat(JsonTrinoConverter.toBigDecimal(1L, createDecimalType(18, 8)).toPlainString())
                    .isEqualTo("0.00000001");
            assertThat(JsonTrinoConverter.toBigDecimal(Int128.valueOf("-99999999999999999999999999999999999999"), createDecimalType(38, 10)).toPlainString())
                    .isEqualTo("-9999999999999999999999999999.9999999999");
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
    }

    private static Object formatTimestamp(TimestampType type, Object value, Schema<?> schema)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return JsonTrinoConverter.convert(blockBuilder.build(), 0, type, schema, new ObjectMapper());
    }
}
