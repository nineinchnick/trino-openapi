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

package io.starburst;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.TimestampType;
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
        if (jsonNode == null) {
            return null;
        }
        if (type instanceof IntegerType) {
            return jsonNode.asInt();
        }
        if (type instanceof BigintType) {
            return jsonNode.bigIntegerValue();
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
            BlockBuilder rowBuilder = blockBuilder.beginBlockEntry();
            writeRow(rowBuilder, jsonNode, rowType, schemaType);
            blockBuilder.closeEntry();
            pageBuilder.declarePosition();
            return rowType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
        }
        throw new RuntimeException(format("Unsupported type %s", type.getClass().getCanonicalName()));
    }

    private static Block buildArray(ArrayNode jsonArray, ArrayType arrayType, Schema<?> schemaType)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(arrayType));

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

        for (JsonNode listObject : jsonArray) {
            if (arrayType.getElementType() instanceof RowType rowType) {
                writeRow(entryBuilder, listObject, rowType, schemaType.getItems());
            }
            else {
                Object value = convert(listObject, arrayType.getElementType(), schemaType.getItems());
                writeTo(entryBuilder, value, arrayType.getElementType());
            }
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    private static Block buildMap(JsonNode node, MapType mapType, Schema<?> schemaType)
    {
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, node.size());
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        node.fields().forEachRemaining(entry -> {
            VARCHAR.writeString(entryBuilder, entry.getKey());
            Object value = convert(entry.getValue(), mapType.getValueType(), (Schema<?>) schemaType.getAdditionalProperties());
            writeTo(entryBuilder, value, mapType.getValueType());
        });
        blockBuilder.closeEntry();
        return mapType.getObject(blockBuilder, 0);
    }

    private static void writeRow(BlockBuilder blockBuilder, JsonNode node, RowType rowType, Schema<?> schemaType)
    {
        if (node == null) {
            blockBuilder.appendNull();
            return;
        }
        BlockBuilder rowBuilder = blockBuilder.beginBlockEntry();
        // iterate over subfields, same as we build the rowType
        List<Map.Entry<String, Schema>> fieldTypes = schemaType.getProperties().entrySet().stream().toList();
        IntStream.range(0, fieldTypes.size()).forEach(i -> {
            String fieldName = fieldTypes.get(i).getKey();
            Schema fieldSchema = fieldTypes.get(i).getValue();
            Type fieldType = rowType.getTypeParameters().get(i);
            Object value = convert(node.get(fieldName), fieldType, fieldSchema);
            writeTo(rowBuilder, value, fieldType);
        });
        blockBuilder.closeEntry();
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
