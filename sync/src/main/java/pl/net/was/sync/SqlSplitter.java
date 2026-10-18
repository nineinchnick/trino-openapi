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

package pl.net.was.sync;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Splits a script into statements at semicolons that are not in string literals, quoted identifiers, or comments.
 * Parts that contain only comments are dropped.
 */
final class SqlSplitter
{
    private SqlSplitter() {}

    static List<String> split(String script)
    {
        ImmutableList.Builder<String> statements = ImmutableList.builder();
        StringBuilder current = new StringBuilder();
        boolean hasCode = false;
        int i = 0;
        while (i < script.length()) {
            char c = script.charAt(i);
            int end = switch (c) {
                case '\'', '"' -> closingQuote(script, i, c);
                case '-' -> script.startsWith("--", i) ? lineEnd(script, i) : i + 1;
                case '/' -> script.startsWith("/*", i) ? blockCommentEnd(script, i) : i + 1;
                default -> i + 1;
            };
            boolean comment = script.startsWith("--", i) || script.startsWith("/*", i);
            if (c == ';') {
                addStatement(statements, current, hasCode);
                current.setLength(0);
                hasCode = false;
                i = end;
                continue;
            }
            current.append(script, i, end);
            hasCode |= !comment && !Character.isWhitespace(c);
            i = end;
        }
        addStatement(statements, current, hasCode);
        return statements.build();
    }

    private static void addStatement(ImmutableList.Builder<String> statements, StringBuilder statement, boolean hasCode)
    {
        if (hasCode) {
            statements.add(statement.toString().strip());
        }
    }

    private static int closingQuote(String script, int start, char quote)
    {
        int end = script.indexOf(quote, start + 1);
        checkArgument(end >= 0, "Unterminated %s at position %s", quote == '\'' ? "string literal" : "quoted identifier", start);
        return end + 1;
    }

    private static int lineEnd(String script, int start)
    {
        int end = script.indexOf('\n', start);
        return end < 0 ? script.length() : end;
    }

    private static int blockCommentEnd(String script, int start)
    {
        int end = script.indexOf("*/", start + 2);
        checkArgument(end >= 0, "Unterminated comment at position %s", start);
        return end + 2;
    }
}
