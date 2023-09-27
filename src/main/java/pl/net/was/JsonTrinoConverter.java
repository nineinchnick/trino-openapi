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
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class JsonTrinoConverter
{
    private JsonTrinoConverter()
    {
    }

    public static Object convert(JsonNode jsonNode, Type type, Schema<?> schemaType)
    {
        if (jsonNode == null || jsonNode instanceof NullNode) {
            return null;
        }
        if (type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
            return jsonNode.asInt();
        }
        if (type instanceof BigintType) {
            return jsonNode.bigIntegerValue();
        }
        if (type instanceof RealType) {
            return jsonNode.floatValue();
        }
        if (type instanceof DoubleType) {
            return jsonNode.doubleValue();
        }
        if (type instanceof VarcharType) {
            return jsonNode.asText();
        }
        if (type instanceof DateType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            TemporalAccessor temporalAccessor = dateFormatter.parse(jsonNode.asText());
            return getSqlDate(LocalDate.from(temporalAccessor));
        }
        if (type instanceof TimestampType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            TemporalAccessor temporalAccessor = dateFormatter.parse(jsonNode.asText());
            if (temporalAccessor instanceof Instant instant) {
                return instant.toEpochMilli();
            }
            if (temporalAccessor instanceof OffsetDateTime offsetDateTime) {
                return offsetDateTime.toEpochSecond();
            }
            if (temporalAccessor instanceof ZonedDateTime zonedDateTime) {
                return zonedDateTime.toEpochSecond();
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unsupported TemporalAccessor type %s", temporalAccessor.getClass().getCanonicalName()));
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

    public static Object convert(Block block, int position, Type type, Schema<?> schemaType, ObjectMapper objectMapper)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            return type.getLong(block, position);
        }
        if (type instanceof DoubleType || type instanceof RealType) {
            return type.getDouble(block, position);
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
            Block arrayBlock = arrayType.getObject(block, position);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object itemValue = convert(arrayBlock, i, arrayType.getElementType(), schemaType.getItems(), objectMapper);
                if (itemValue == null) {
                    arrayNode.addNull();
                }
                else if (itemValue instanceof Boolean booleanValue) {
                    arrayNode.add(booleanValue);
                }
                else if (itemValue instanceof Long longValue) {
                    arrayNode.add(longValue);
                }
                else if (itemValue instanceof Double doubleValue) {
                    arrayNode.add(doubleValue);
                }
                else if (itemValue instanceof String stringValue) {
                    arrayNode.add(stringValue);
                }
                else if (itemValue instanceof ArrayNode || itemValue instanceof ObjectNode) {
                    arrayNode.add((JsonNode) itemValue);
                }
                else {
                    throw new RuntimeException(format("Unsupported object of class %s", itemValue.getClass()));
                }
            }
            return arrayNode;
        }
        if (type instanceof MapType mapType) {
            SingleMapBlock mapBlock = (SingleMapBlock) mapType.getObject(block, position);
            ObjectNode mapNode = objectMapper.createObjectNode();
            int entryCount = mapBlock.getPositionCount() / 2;
            for (int i = 0; i < entryCount; i++) {
                // TODO passing schemaType.getItems() to both key and value is probably incorrect
                String key = convert(mapBlock, 2 * i, mapType.getKeyType(), schemaType.getItems(), objectMapper).toString();
                Object itemValue = convert(mapBlock, 2 * i + 1, mapType.getValueType(), schemaType.getItems(), objectMapper);
                if (itemValue == null) {
                    mapNode.putNull(key);
                }
                else if (itemValue instanceof Boolean booleanValue) {
                    mapNode.put(key, booleanValue);
                }
                else if (itemValue instanceof Long longValue) {
                    mapNode.put(key, longValue);
                }
                else if (itemValue instanceof Double doubleValue) {
                    mapNode.put(key, doubleValue);
                }
                else if (itemValue instanceof String stringValue) {
                    mapNode.put(key, stringValue);
                }
                else if (itemValue instanceof ArrayNode || itemValue instanceof ObjectNode) {
                    mapNode.set(key, (JsonNode) itemValue);
                }
                else {
                    throw new RuntimeException(format("Unsupported object of class %s", itemValue.getClass()));
                }
            }
            return mapNode;
        }
        if (type instanceof RowType rowType) {
            SingleRowBlock rowBlock = (SingleRowBlock) rowType.getObject(block, position);
            ObjectNode rowNode = objectMapper.createObjectNode();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                RowType.Field field = rowType.getFields().get(i);
                String key = field.getName().orElse(format("_col%d", i));
                // TODO passing schemaType.getItems() is incorrect, need to use getProperties().get(originalFieldName)
                Object itemValue = convert(rowBlock, i, field.getType(), schemaType.getItems(), objectMapper);
                if (itemValue == null) {
                    rowNode.putNull(key);
                }
                else if (itemValue instanceof Boolean booleanValue) {
                    rowNode.put(key, booleanValue);
                }
                else if (itemValue instanceof Long longValue) {
                    rowNode.put(key, longValue);
                }
                else if (itemValue instanceof Double doubleValue) {
                    rowNode.put(key, doubleValue);
                }
                else if (itemValue instanceof String stringValue) {
                    rowNode.put(key, stringValue);
                }
                else if (itemValue instanceof ArrayNode || itemValue instanceof ObjectNode) {
                    rowNode.set(key, (JsonNode) itemValue);
                }
                else {
                    throw new RuntimeException(format("Unsupported object of class %s", itemValue.getClass()));
                }
            }
            return rowNode;
        }
        throw new RuntimeException(format("Unsupported type %s (%s)", type.getDisplayName(), type.getClass().getCanonicalName()));
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

    private static Block buildMap(JsonNode node, MapType mapType, Schema<?> schemaType)
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
        if (type instanceof VarcharType varcharType) {
            varcharType.writeString(rowBuilder, (String) value);
            return;
        }
        if (type instanceof IntegerType integerType) {
            integerType.writeLong(rowBuilder, ((Integer) value).longValue());
            return;
        }
        if (type instanceof BigintType bigintType) {
            bigintType.writeLong(rowBuilder, (Long) value);
            return;
        }
        if (type instanceof TimestampType timestampType) {
            // TODO check precision
            timestampType.writeLong(rowBuilder, packTimestamp((ZonedDateTime) value));
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

    public static SqlDate getSqlDate(LocalDate localDate)
    {
        return new SqlDate((int) localDate.toEpochDay());
    }
}
