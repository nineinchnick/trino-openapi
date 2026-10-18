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

import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.toIntExact;

public class DateTimeConverter
{
    private DateTimeConverter()
    {
    }

    public static int parseDate(String text, Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format.equals("date")) {
            format = "yyyy-MM-dd";
        }
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
        return toIntExact(dateFormatter.parse(text, LocalDate::from).toEpochDay());
    }

    public static long parseTimestamp(String text, TimestampType type, Schema<?> schema)
    {
        String format = schema.getFormat();
        DateTimeFormatter dateFormatter;
        if (format.equals("date-time")) {
            dateFormatter = DateTimeFormatter.ISO_DATE_TIME;
        }
        else {
            dateFormatter = DateTimeFormatter.ofPattern(format);
        }

        Instant instant = dateFormatter.parse(text, Instant::from);
        long epochMicros = instant.toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
        return Timestamps.round(epochMicros, 6 - type.getPrecision());
    }

    public static String formatDate(long days, Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format.equals("date")) {
            format = "yyyy-MM-dd";
        }
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
        return dateFormatter.format(LocalDate.ofEpochDay(days));
    }

    public static String formatTimestamp(long millis, Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format.equals("date-time")) {
            format = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]XXX";
        }
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
        return dateFormatter.format(Instant.ofEpochMilli(millis));
    }
}
