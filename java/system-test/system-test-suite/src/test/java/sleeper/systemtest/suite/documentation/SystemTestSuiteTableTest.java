/*
 * Copyright 2026 Crown Copyright
 *
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
package sleeper.systemtest.suite.documentation;

import org.junit.jupiter.api.Test;

import sleeper.systemtest.suite.testutil.parallel.Slow1;
import sleeper.systemtest.suite.testutil.parallel.Slow2;
import sleeper.systemtest.suite.testutil.parallel.Slow3;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SystemTestSuiteTableTest {

    @Test
    void shouldCreateTableForSystemTestSuites() {
        // When
        String table = SystemTestSuiteTable.create(
                List.of(Slow1.class, Slow2.class, Slow3.class),
                List.of(ExampleSlow1ST.class, ExampleSlow3ST.class));

        // Then
        assertThat(table).isEqualTo("""
                | Slow1          | Slow2 | Slow3          |
                |----------------|-------|----------------|
                | ExampleSlow1ST |       | ExampleSlow3ST |""");
    }

    @Test
    void shouldSortTestsAndRemoveTrailingEmptyColumns() {
        // When
        String table = SystemTestSuiteTable.create(
                List.of(Slow1.class, Slow2.class, Slow3.class),
                List.of(ZebraSlow1ST.class, ExampleSlow2ST.class, AlphaSlow1ST.class));

        // Then
        assertThat(table).isEqualTo("""
                | Slow1        | Slow2          | Slow3 |
                |--------------|----------------|-------|
                | AlphaSlow1ST | ExampleSlow2ST |
                | ZebraSlow1ST |""");
    }

    @Slow1
    private static class ExampleSlow1ST {
    }

    @Slow3
    private static class ExampleSlow3ST {
    }

    @Slow1
    private static class AlphaSlow1ST {
    }

    @Slow1
    private static class ZebraSlow1ST {
    }

    @Slow2
    private static class ExampleSlow2ST {
    }
}
