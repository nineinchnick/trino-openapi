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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum OpenApiErrorCode
        implements ErrorCodeSupplier
{
    OPENAPI_HTTP_ERROR(0, EXTERNAL),
    OPENAPI_CONNECTION_ERROR(1, EXTERNAL),
    OPENAPI_INVALID_RESPONSE(2, EXTERNAL),
    OPENAPI_SPEC_MISMATCH(3, EXTERNAL);

    // codes are part of the client protocol, so they must not change, and the offset must not be used by Trino plugins

    private static final int OFFSET = 0x0700_0000;

    private final ErrorCode errorCode;

    OpenApiErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + OFFSET, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
