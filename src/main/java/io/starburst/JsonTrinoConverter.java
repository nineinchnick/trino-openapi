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

import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.json.JSONArray;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import static java.lang.String.format;

public class JsonTrinoConverter
{
    private JsonTrinoConverter()
    {
    }

    public static Object convert(Object jsonObject, Type type, Schema<?> schemaType)
    {
        if (type instanceof IntegerType) {
            return jsonObject;
        }
        if (type instanceof BigintType) {
            return jsonObject;
        }
        if (type instanceof VarcharType) {
            return jsonObject;
        }
        if (type instanceof DateType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            TemporalAccessor temporalAccessor = dateFormatter.parse(jsonObject.toString());
            return getSqlDate(LocalDate.from(temporalAccessor));
        }
        if (type instanceof TimestampType) {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(schemaType.getFormat());
            TemporalAccessor temporalAccessor = dateFormatter.parse(jsonObject.toString());
            if (temporalAccessor instanceof Instant) {
                return ((Instant) temporalAccessor).toEpochMilli();
            }
            if (temporalAccessor instanceof OffsetDateTime) {
                return ((OffsetDateTime) temporalAccessor).toEpochSecond();
            }
            if (temporalAccessor instanceof ZonedDateTime) {
                return ((ZonedDateTime) temporalAccessor).toEpochSecond();
            }
            throw new RuntimeException(format("Unsupported TemporalAccessor type %s", temporalAccessor.getClass().getCanonicalName()));
        }
        if (type instanceof BooleanType) {
            return jsonObject;
        }
        if (type instanceof MapType) {
            throw new RuntimeException("MapType unsupported currently");
        }
        if (type instanceof ArrayType) {
            JSONArray jsonArray = (JSONArray) jsonObject;
            // TODO this should be a Trino Block
            ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
            for (Object listObject : jsonArray) {
                listBuilder.add(convert(listObject, ((ArrayType) type).getElementType(), schemaType.getItems()));
            }
            return listBuilder.build();
        }
        throw new RuntimeException(format("Unsupported type %s", type.getClass().getCanonicalName()));
    }

    public static SqlDate getSqlDate(LocalDate localDate)
    {
        return new SqlDate((int) localDate.toEpochDay());
    }
}
