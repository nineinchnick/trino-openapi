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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Tag("live-api")
public class TestOpenMeteo
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenApiQueryRunner.createQueryRunner(Map.of(
                "openmeteo", Map.of(
                        "spec-location", "src/test/resources/openmeteo.yml",
                        "base-uri", "https://api.open-meteo.com")));
    }

    @Test
    public void selectFromForecastTable()
    {
        // latitude and longitude are strings in requests, since they can hold several comma separated coordinates
        assertQuery(
                "SELECT elevation, timezone, current.temperature_2m BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = '53.1325' AND longitude_req = '23.1688'",
                "VALUES (135.0, 'GMT', null)");
        assertQuery(
                "SELECT elevation, timezone, current.temperature_2m BETWEEN -50 AND 100 AS is_livable, cardinality(hourly.temperature_2m) " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = '53.1325' AND longitude_req = '23.1688' " +
                        "AND current_req = ARRAY['temperature_2m'] AND hourly_req = ARRAY['temperature_2m', 'weather_code'] AND forecast_days = 1",
                "VALUES (135.0, 'GMT', true, 24)");
    }
}
