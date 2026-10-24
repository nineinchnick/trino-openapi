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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestLinkHeader
{
    @Test
    void testGithubLinks()
    {
        assertThat(LinkHeader.getNextLink(List.of(
                "<https://api.github.com/repositories/1/issues?page=2>; rel=\"next\", <https://api.github.com/repositories/1/issues?page=5>; rel=\"last\"")))
                .contains("https://api.github.com/repositories/1/issues?page=2");
        // the last page links only to the previous and the first ones
        assertThat(LinkHeader.getNextLink(List.of(
                "<https://api.github.com/repositories/1/issues?page=4>; rel=\"prev\", <https://api.github.com/repositories/1/issues?page=1>; rel=\"first\"")))
                .isEmpty();
    }

    @Test
    void testRelations()
    {
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>; rel=next"))).contains("/items?cursor=b");
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>;rel=\"NEXT\""))).contains("/items?cursor=b");
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>; rel=\"last next\""))).contains("/items?cursor=b");
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>; rel=\"next-archive\""))).isEmpty();
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>; title=\"next\""))).isEmpty();
        // only the first rel parameter is used
        assertThat(LinkHeader.getNextLink(List.of("</items?cursor=b>; rel=prev; rel=next"))).isEmpty();
    }

    @Test
    void testQuotedParameters()
    {
        assertThat(LinkHeader.getNextLink(List.of("</items?page=1>; title=\"first, then; next\"; rel=\"first\", </items?page=2>; rel=\"next\"")))
                .contains("/items?page=2");
    }

    @Test
    void testMultipleHeaders()
    {
        assertThat(LinkHeader.getNextLink(List.of("</items?page=1>; rel=\"first\"", "</items?page=2>; rel=\"next\"")))
                .contains("/items?page=2");
        assertThat(LinkHeader.getNextLink(List.of())).isEmpty();
        assertThat(LinkHeader.getNextLink(List.of("not a link"))).isEmpty();
    }
}
