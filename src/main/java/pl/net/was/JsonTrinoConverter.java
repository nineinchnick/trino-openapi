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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public class JsonTrinoConverter
{
    private JsonTrinoConverter()
    {
    }

    /**
     * Converts a JSON value into the representation that {@link io.trino.spi.connector.InMemoryRecordSet} expects for a top-level column.
     */
    public static Object convertToRecordValue(JsonNode jsonNode, Type type, Schema<?> schemaType)
    {
        Object value = convert(jsonNode, type, schemaType);
        if (type instanceof RealType && value instanceof Float floatValue) {
            // records read REAL values with getLong(), so they must hold the float bits, like RealType.writeFloat() does for nested values
            return (long) floatToIntBits(floatValue);
        }
        return value;
    }

    public static Object convert(JsonNode jsonNode, Type type, Schema<?> schemaType)
    {
        if (jsonNode == null || jsonNode instanceof NullNode) {
            return null;
        }
        if (type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
            return jsonNode.isTextual() ? parseText(jsonNode, type, Integer::valueOf) : jsonNode.asInt();
        }
        if (type instanceof BigintType) {
            return jsonNode.isTextual() ? parseText(jsonNode, type, BigInteger::new) : jsonNode.bigIntegerValue();
        }
        if (type instanceof RealType) {
            return jsonNode.isTextual() ? parseText(jsonNode, type, Float::valueOf) : jsonNode.floatValue();
        }
        if (type instanceof DoubleType) {
            return jsonNode.isTextual() ? parseText(jsonNode, type, Double::valueOf) : jsonNode.doubleValue();
        }
        if (type instanceof DecimalType decimalType) {
            // some APIs send decimals as JSON strings, so clients that parse numbers as doubles don't lose precision
            BigDecimal value = jsonNode.isTextual() ? parseText(jsonNode, type, BigDecimal::new) : jsonNode.decimalValue();
            return toUnscaledValue(value, decimalType);
        }
        if (type instanceof VarcharType) {
            if (jsonNode.isValueNode()) {
                return jsonNode.asText();
            }
            // fallback for unknown/invalid types
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(jsonNode);
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to serialize node to string: %s", jsonNode));
            }
        }
        if (type instanceof DateType) {
            String format = schemaType.getFormat();
            if (format.equals("date")) {
                format = "yyyy-MM-dd";
            }
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
            return toIntExact(dateFormatter.parse(jsonNode.asText(), LocalDate::from).toEpochDay());
        }
        if (type instanceof TimestampType timestampType) {
            String format = schemaType.getFormat();
            DateTimeFormatter dateFormatter;
            if (format.equals("date-time")) {
                dateFormatter = DateTimeFormatter.ISO_DATE_TIME;
            }
            else {
                dateFormatter = DateTimeFormatter.ofPattern(format);
            }

            Instant instant = dateFormatter.parse(jsonNode.asText(), Instant::from);
            long epochMicros = instant.toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
            return Timestamps.round(epochMicros, 6 - timestampType.getPrecision());
        }
        if (type instanceof BooleanType) {
            return jsonNode.asBoolean();
        }
        if (type instanceof MapType mapType) {
            return buildMap(jsonNode, mapType, schemaType);
        }
        if (type instanceof ArrayType arrayType) {
            return buildArray((ArrayNode) jsonNode, arrayType, schemaType);
        }
        if (type instanceof RowType rowType) {
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(rowType));
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            writeRow(blockBuilder, jsonNode, rowType, schemaType);
            pageBuilder.declarePosition();
            Block block = blockBuilder.build();
            return rowType.getObject(block, block.getPositionCount() - 1);
        }
        throw new RuntimeException(format("Unsupported type %s", type.getClass().getCanonicalName()));
    }

    private static <T> T parseText(JsonNode jsonNode, Type type, Function<String, T> parser)
    {
        try {
            return parser.apply(jsonNode.textValue());
        }
        catch (NumberFormatException e) {
            throw new TrinoException(TYPE_MISMATCH, format("Cannot read '%s' as %s", jsonNode.textValue(), type), e);
        }
    }

    private static Object toUnscaledValue(BigDecimal value, DecimalType type)
    {
        BigDecimal rescaled = value.setScale(type.getScale(), HALF_UP);
        if (overflows(rescaled, type.getPrecision())) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value %s doesn't fit in %s", value.toPlainString(), type));
        }
        if (type.isShort()) {
            return encodeShortScaledValue(rescaled, type.getScale());
        }
        return encodeScaledValue(rescaled, type.getScale());
    }

    private static Block buildArray(ArrayNode jsonArray, ArrayType arrayType, Schema<?> schemaType)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(arrayType));

        ArrayBlockBuilder blockBuilder = (ArrayBlockBuilder) pageBuilder.getBlockBuilder(0);
        blockBuilder.buildEntry(elementBuilder -> {
            for (JsonNode listObject : jsonArray) {
                if (arrayType.getElementType() instanceof RowType rowType) {
                    writeRow(elementBuilder, listObject, rowType, schemaType.getItems());
                }
                else {
                    Object value = convert(listObject, arrayType.getElementType(), schemaType.getItems());
                    writeTo(elementBuilder, value, arrayType.getElementType());
                }
            }
        });

        pageBuilder.declarePosition();
        Block block = blockBuilder.build();
        return arrayType.getObject(block, block.getPositionCount() - 1);
    }

    private static SqlMap buildMap(JsonNode node, MapType mapType, Schema<?> schemaType)
    {
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, node.size());
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            node.fields().forEachRemaining(entry -> {
                VARCHAR.writeString(keyBuilder, entry.getKey());
                Object value = convert(entry.getValue(), mapType.getValueType(), (Schema<?>) schemaType.getAdditionalProperties());
                writeTo(valueBuilder, value, mapType.getValueType());
            });
        });
        Block block = blockBuilder.build();
        return mapType.getObject(block, 0);
    }

    private static void writeRow(BlockBuilder blockBuilder, JsonNode node, RowType rowType, Schema<?> schemaType)
    {
        if (node == null) {
            blockBuilder.appendNull();
            return;
        }
        RowBlockBuilder rowBuilder = (RowBlockBuilder) blockBuilder;
        rowBuilder.buildEntry(fieldBuilders -> {
            // iterate over subfields, same as we build the rowType
            List<Map.Entry<String, Schema>> fieldTypes = schemaType.getProperties().entrySet().stream().toList();
            IntStream.range(0, fieldTypes.size()).forEach(i -> {
                String fieldName = fieldTypes.get(i).getKey();
                Schema fieldSchema = fieldTypes.get(i).getValue();
                Type fieldType = rowType.getTypeParameters().get(i);
                Object value = convert(node.get(fieldName), fieldType, fieldSchema);
                writeTo(fieldBuilders.get(i), value, fieldType);
            });
        });
    }

    private static void writeTo(BlockBuilder rowBuilder, Object value, Type type)
    {
        if (value == null) {
            rowBuilder.appendNull();
            return;
        }
        if (type instanceof BooleanType booleanType) {
            booleanType.writeBoolean(rowBuilder, (Boolean) value);
            return;
        }
        if (type instanceof VarcharType varcharType) {
            varcharType.writeString(rowBuilder, (String) value);
            return;
        }
        if (type instanceof IntegerType integerType) {
            integerType.writeLong(rowBuilder, ((Integer) value).longValue());
            return;
        }
        if (type instanceof BigintType bigintType) {
            bigintType.writeLong(rowBuilder, ((BigInteger) value).longValue());
            return;
        }
        if (type instanceof RealType realType) {
            realType.writeFloat(rowBuilder, (Float) value);
            return;
        }
        if (type instanceof DoubleType doubleType) {
            doubleType.writeDouble(rowBuilder, (Double) value);
            return;
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                decimalType.writeLong(rowBuilder, (Long) value);
            }
            else {
                decimalType.writeObject(rowBuilder, value);
            }
            return;
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                timestampType.writeLong(rowBuilder, (Long) value);
            }
            else {
                timestampType.writeLong(rowBuilder, packTimestamp((ZonedDateTime) value));
            }
            return;
        }
        if (type instanceof DateType dateType) {
            dateType.writeInt(rowBuilder, (Integer) value);
            return;
        }
        if (type instanceof MapType mapType) {
            mapType.writeObject(rowBuilder, value);
            return;
        }
        if (type instanceof ArrayType arrayType) {
            arrayType.writeObject(rowBuilder, value);
            return;
        }
        if (type instanceof RowType rowType) {
            rowType.writeObject(rowBuilder, value);
            return;
        }
        throw new RuntimeException(format("Unsupported array element type %s", type.getClass().getCanonicalName()));
    }

    private static long packTimestamp(ZonedDateTime timestamp)
    {
        if (timestamp == null) {
            return 0;
        }
        return DateTimeEncoding.packDateTimeWithZone(
                timestamp.toEpochSecond() * MILLISECONDS_PER_SECOND + roundDiv(timestamp.toLocalTime().getNano(), NANOSECONDS_PER_MILLISECOND),
                timestamp.getZone().getId());
    }

    public static Object convert(Block block, int position, Type type, Schema<?> schemaType, ObjectMapper objectMapper)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (schemaType.getOneOf() != null
                || schemaType.getAnyOf() != null
                || schemaType.getAllOf() != null) {
            verify(type instanceof VarcharType, "OpenAPI union types must map to Trino VARCHAR");
            return new RawValue(type.getSlice(block, position).toStringUtf8());
        }
        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            return type.getLong(block, position);
        }
        if (type instanceof RealType realType) {
            return realType.getFloat(block, position);
        }
        if (type instanceof DoubleType) {
            return type.getDouble(block, position);
        }
        if (type instanceof DecimalType decimalType) {
            Object unscaledValue = decimalType.isShort() ? decimalType.getLong(block, position) : decimalType.getObject(block, position);
            return toBigDecimal(unscaledValue, decimalType);
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof DateType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            return dateFormatter.format(LocalDate.ofEpochDay(type.getLong(block, position)));
        }
        if (type instanceof TimestampType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            // TODO this is probably wrong, handle other types/precisions
            return dateFormatter.format(Instant.ofEpochMilli(type.getLong(block, position)));
        }
        if (type instanceof ArrayType arrayType) {
            if (block instanceof ArrayBlock arrayBlock) {
                Schema<?> finalSchemaType = schemaType;
                return arrayBlock.apply((valuesBlock, start, length) -> convertArray(valuesBlock, start, length, arrayType, finalSchemaType, objectMapper), position);
            }
            return convertArray(block, 0, block.getPositionCount(), arrayType, schemaType, objectMapper);
        }
        if (type instanceof MapType mapType) {
            SqlMap mapBlock = mapType.getObject(block, position);
            return convertMap(mapBlock, mapType, schemaType, objectMapper);
        }
        if (type instanceof RowType rowType) {
            ObjectNode rowNode = objectMapper.createObjectNode();
            if (block.getPositionCount() == 0) {
                return rowNode;
            }
            SqlRow rowBlock = rowType.getObject(block, position);
            convertRow(rowNode, rowBlock, rowType, schemaType, objectMapper);
            return rowNode;
        }
        throw new RuntimeException(format("Unsupported type %s (%s)", type.getDisplayName(), type.getClass().getCanonicalName()));
    }

    /**
     * Converts the unscaled value of a short ({@code long}) or long ({@link Int128}) decimal into an exact {@link BigDecimal}.
     */
    public static BigDecimal toBigDecimal(Object unscaledValue, DecimalType type)
    {
        BigInteger unscaled = switch (unscaledValue) {
            case Long longValue -> BigInteger.valueOf(longValue);
            case Int128 int128Value -> int128Value.toBigInteger();
            default -> throw new IllegalArgumentException(format("Unsupported unscaled value of class %s for %s", unscaledValue.getClass(), type));
        };
        return new BigDecimal(unscaled, type.getScale());
    }

    private static ArrayNode convertArray(Block arrayBlock, int start, int length, ArrayType arrayType, Schema<?> schemaType, ObjectMapper objectMapper)
    {
        ArrayNode arrayNode = objectMapper.createArrayNode();
        for (int i = 0; i < length; i++) {
            Object itemValue = convert(arrayBlock, i + start, arrayType.getElementType(), schemaType.getItems(), objectMapper);
            if (itemValue == null) {
                arrayNode.addNull();
            }
            else if (itemValue instanceof Boolean booleanValue) {
                arrayNode.add(booleanValue);
            }
            else if (itemValue instanceof Long longValue) {
                arrayNode.add(longValue);
            }
            else if (itemValue instanceof Float floatValue) {
                arrayNode.add(floatValue);
            }
            else if (itemValue instanceof Double doubleValue) {
                arrayNode.add(doubleValue);
            }
            else if (itemValue instanceof BigDecimal decimalValue) {
                arrayNode.add(decimalValue);
            }
            else if (itemValue instanceof String stringValue) {
                arrayNode.add(stringValue);
            }
            else if (itemValue instanceof ArrayNode || itemValue instanceof ObjectNode) {
                arrayNode.add((JsonNode) itemValue);
            }
            else if (itemValue instanceof RawValue rawValue) {
                arrayNode.addRawValue(rawValue);
            }
            else {
                throw new RuntimeException(format("Unsupported object of class %s", itemValue.getClass()));
            }
        }
        return arrayNode;
    }

    private static ObjectNode convertMap(SqlMap mapBlock, MapType mapType, Schema<?> schemaType, ObjectMapper objectMapper)
    {
        ObjectNode mapNode = objectMapper.createObjectNode();
        // JSON object keys are always strings, so map types are created only with VARCHAR keys
        Schema<?> valueSchema = (Schema<?>) schemaType.getAdditionalProperties();
        for (int i = 0; i < mapBlock.getSize(); i++) {
            String key = VARCHAR.getSlice(mapBlock.getRawKeyBlock(), mapBlock.getRawOffset() + i).toStringUtf8();
            Object itemValue = convert(mapBlock.getRawValueBlock(), mapBlock.getRawOffset() + i, mapType.getValueType(), valueSchema, objectMapper);
            if (itemValue == null) {
                mapNode.putNull(key);
            }
            else if (itemValue instanceof Boolean booleanValue) {
                mapNode.put(key, booleanValue);
            }
            else if (itemValue instanceof Long longValue) {
                mapNode.put(key, longValue);
            }
            else if (itemValue instanceof Float floatValue) {
                mapNode.put(key, floatValue);
            }
            else if (itemValue instanceof Double doubleValue) {
                mapNode.put(key, doubleValue);
            }
            else if (itemValue instanceof BigDecimal decimalValue) {
                mapNode.put(key, decimalValue);
            }
            else if (itemValue instanceof String stringValue) {
                mapNode.put(key, stringValue);
            }
            else if (itemValue instanceof ArrayNode || itemValue instanceof ObjectNode) {
                mapNode.set(key, (JsonNode) itemValue);
            }
            else if (itemValue instanceof RawValue rawValue) {
                mapNode.putRawValue(key, rawValue);
            }
            else {
                throw new RuntimeException(format("Unsupported object of class %s", itemValue.getClass()));
            }
        }
        return mapNode;
    }

    public static void convertRow(ObjectNode rowNode, SqlRow row, RowType rowType, Schema<?> schemaType, ObjectMapper objectMapper)
    {
        for (int i = 0; i < row.getFieldCount(); i++) {
            RowType.Field field = rowType.getFields().get(i);
            String key = field.getName().orElse(format("_col%d", i));
            Object value = convert(row.getRawFieldBlock(i), row.getRawIndex(), field.getType(), schemaType.getProperties().get(key), objectMapper);
            if (value == null) {
                rowNode.putNull(key);
            }
            else if (value instanceof Boolean booleanValue) {
                rowNode.put(key, booleanValue);
            }
            else if (value instanceof Long longValue) {
                rowNode.put(key, longValue);
            }
            else if (value instanceof Float floatValue) {
                rowNode.put(key, floatValue);
            }
            else if (value instanceof Double doubleValue) {
                rowNode.put(key, doubleValue);
            }
            else if (value instanceof BigDecimal decimalValue) {
                rowNode.put(key, decimalValue);
            }
            else if (value instanceof String stringValue) {
                rowNode.put(key, stringValue);
            }
            else if (value instanceof ArrayNode || value instanceof ObjectNode) {
                rowNode.set(key, (JsonNode) value);
            }
            else {
                throw new RuntimeException(format("Unsupported object of class %s", value.getClass()));
            }
        }
    }
}
