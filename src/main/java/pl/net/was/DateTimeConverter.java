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
import io.trino.spi.TrinoException;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNullElse;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;

/**
 * Converts between text in the formats of OpenAPI schemas and native values of DATE and TIMESTAMP columns.
 * TIMESTAMP columns hold the UTC wall-clock time, so values with an offset are shifted to UTC when parsed,
 * and values without one are read as UTC.
 */
public class DateTimeConverter
{
    private static final int MAX_FRACTION_DIGITS = 9;
    private static final List<DateTimeFormatter> RFC_3339_FORMATTERS = IntStream.rangeClosed(0, MAX_FRACTION_DIGITS)
            .mapToObj(DateTimeConverter::rfc3339Formatter)
            .collect(toImmutableList());

    private DateTimeConverter() {}

    public static int parseDate(String text, Schema<?> schema)
    {
        LocalDate date = parse(text, DateType.DATE, dateFormatter(schema), LocalDate::from);
        return toIntExact(date.toEpochDay());
    }

    /**
     * Returns epoch microseconds for short timestamps, and a {@link LongTimestamp} for long timestamps.
     */
    public static Object parseTimestamp(String text, TimestampType type, Schema<?> schema)
    {
        LocalDateTime dateTime = parse(text, type, dateTimeParser(schema), DateTimeConverter::toUtcDateTime);
        return toNativeValue(dateTime, type);
    }

    public static String formatDate(long epochDay, Schema<?> schema)
    {
        return dateFormatter(schema).format(LocalDate.ofEpochDay(epochDay));
    }

    public static String formatTimestamp(Object nativeValue, TimestampType type, Schema<?> schema)
    {
        LocalDateTime dateTime = switch (nativeValue) {
            case Long epochMicros -> toUtcDateTime(epochMicros, 0);
            case LongTimestamp timestamp -> toUtcDateTime(timestamp.getEpochMicros(), timestamp.getPicosOfMicro());
            default -> throw new IllegalArgumentException(format("Unsupported native value of class %s for %s", nativeValue.getClass(), type));
        };
        return dateTimeFormatter(schema, type).format(dateTime.atZone(UTC));
    }

    private static <T> T parse(String text, Type type, DateTimeFormatter formatter, Function<TemporalAccessor, T> converter)
    {
        try {
            return converter.apply(formatter.parse(text));
        }
        catch (DateTimeException _) {
            // neither the value, nor the parser's exception that quotes it, are included, because the value might be sensitive
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Cannot read a JSON string as %s", type));
        }
    }

    private static DateTimeFormatter dateFormatter(Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format == null || format.equals("date")) {
            return ISO_LOCAL_DATE;
        }
        return customFormatter(format);
    }

    private static DateTimeFormatter dateTimeParser(Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format == null || format.equals("date-time")) {
            // also accepts values without an offset, which RFC 3339 doesn't allow, but some APIs send anyway
            return ISO_DATE_TIME;
        }
        return customFormatter(format);
    }

    private static DateTimeFormatter dateTimeFormatter(Schema<?> schema, TimestampType type)
    {
        String format = schema.getFormat();
        if (format == null || format.equals("date-time")) {
            // java.time can't hold more than nanoseconds, so greater precisions are truncated
            return RFC_3339_FORMATTERS.get(min(type.getPrecision(), MAX_FRACTION_DIGITS));
        }
        return customFormatter(format);
    }

    private static DateTimeFormatter customFormatter(String pattern)
    {
        // names of months and days in APIs are almost always in English, so they can't depend on the default locale
        return DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);
    }

    private static DateTimeFormatter rfc3339Formatter(int fractionDigits)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                .append(ISO_LOCAL_DATE)
                .appendLiteral('T')
                .appendPattern("HH:mm:ss");
        if (fractionDigits > 0) {
            builder.appendFraction(NANO_OF_SECOND, fractionDigits, fractionDigits, true);
        }
        return builder
                .appendOffset("+HH:MM", "Z")
                .toFormatter(Locale.ENGLISH);
    }

    private static LocalDateTime toUtcDateTime(TemporalAccessor parsed)
    {
        if (parsed.isSupported(INSTANT_SECONDS)) {
            return LocalDateTime.ofInstant(Instant.from(parsed), UTC);
        }
        LocalDate date = LocalDate.from(parsed);
        LocalDateTime dateTime = date.atTime(requireNonNullElse(parsed.query(TemporalQueries.localTime()), LocalTime.MIDNIGHT));
        ZoneId zone = parsed.query(TemporalQueries.zone());
        if (zone == null) {
            return dateTime;
        }
        return dateTime.atZone(zone).withZoneSameInstant(UTC).toLocalDateTime();
    }

    private static LocalDateTime toUtcDateTime(long epochMicros, int picosOfMicro)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND + picosOfMicro / PICOSECONDS_PER_NANOSECOND;
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, UTC);
    }

    private static Object toNativeValue(LocalDateTime dateTime, TimestampType type)
    {
        long nanoOfSecond = round(dateTime.get(NANO_OF_SECOND), max(0, MAX_FRACTION_DIGITS - type.getPrecision()));
        // rounding can carry over to the next second, which adding microseconds handles too
        long epochMicros = multiplyExact(dateTime.toEpochSecond(UTC), MICROSECONDS_PER_SECOND) + nanoOfSecond / NANOSECONDS_PER_MICROSECOND;
        if (type.isShort()) {
            return epochMicros;
        }
        return new LongTimestamp(epochMicros, toIntExact(nanoOfSecond % NANOSECONDS_PER_MICROSECOND * PICOSECONDS_PER_NANOSECOND));
    }
}
