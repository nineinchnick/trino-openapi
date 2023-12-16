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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class OpenApiPageSink
        implements ConnectorPageSink
{
    protected final OpenApiClient client;
    protected final OpenApiOutputTableHandle table;

    public OpenApiPageSink(OpenApiClient client, OpenApiOutputTableHandle table)
    {
        this.client = client;
        this.table = table;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            insertedPage(page, position);
        }
        return NOT_BLOCKED;
    }

    protected void insertedPage(Page page, int position)
    {
        PathItem.HttpMethod method = this.table.getTableHandle().getUpdateMethod();
        if (method == PathItem.HttpMethod.POST) {
            client.postRows(table, page, position);
        }
        else if (method == PathItem.HttpMethod.PUT) {
            client.putRows(table, page, position);
        }
        else {
            throw new IllegalArgumentException("Unsupported INSERT method: " + method);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
    }
}
