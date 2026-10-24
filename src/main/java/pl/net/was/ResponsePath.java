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

import com.fasterxml.jackson.core.JsonPointer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public sealed interface ResponsePath
{
    record Body(JsonPointer pointer)
            implements ResponsePath
    {
        public Body
        {
            requireNonNull(pointer, "pointer is null");
        }

        @Override
        public String toString()
        {
            return pointer.toString();
        }
    }

    record Header(String name)
            implements ResponsePath
    {
        public Header
        {
            requireNonNull(name, "name is null");
            checkArgument(!name.isBlank(), "header name is blank");
        }

        @Override
        public String toString()
        {
            return "header " + name;
        }
    }
}
