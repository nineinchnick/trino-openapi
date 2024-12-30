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

import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;

import static io.trino.spi.type.TinyintType.TINYINT;

public class OpenApiMergeSink
        extends OpenApiPageSink
        implements ConnectorMergeSink
{
    public OpenApiMergeSink(OpenApiClient client, OpenApiOutputTableHandle table)
    {
        super(client, table);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        Block rowIds = page.getBlock(page.getChannelCount() - 1);
        Block ops = page.getBlock(page.getChannelCount() - 3);
        for (int position = 0; position < page.getPositionCount(); position++) {
            byte op = TINYINT.getByte(ops, position);
            switch (op) {
                case INSERT_OPERATION_NUMBER -> insertedPage(page, position);
                case UPDATE_OPERATION_NUMBER -> updatedPage(page, position);
                case DELETE_OPERATION_NUMBER -> deletedPage(rowIds, position);
                default -> throw new IllegalStateException("Unsupported operation: " + op);
            }
        }
    }

    private void updatedPage(Page page, int position)
    {
        PathItem.HttpMethod method = this.table.getTableHandle().getUpdateMethod();
        if (method == PathItem.HttpMethod.PUT) {
            client.putRows(table, page, position);
        }
        else if (method == PathItem.HttpMethod.POST) {
            client.postRows(table, page, position);
        }
        else {
            throw new IllegalArgumentException("Unsupported UPDATE method: " + method);
        }
    }

    private void deletedPage(Block rowIds, int position)
    {
        PathItem.HttpMethod method = this.table.getTableHandle().getDeleteMethod();
        if (method == PathItem.HttpMethod.DELETE) {
            client.deleteRows(table, rowIds, position);
        }
        else {
            throw new IllegalArgumentException("Unsupported DELETE method: " + method);
        }
    }
}
