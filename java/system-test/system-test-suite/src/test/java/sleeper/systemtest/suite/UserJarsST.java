/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;

import java.util.Map;

import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;

@Disabled("TODO")
public class UserJarsST {

    @Test
    void shouldApplyTableIteratorDuringCompaction(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.updateTableProperties(Map.of(
                ITERATOR_CLASS_NAME, "sleeper.example.iterator.FixedAgeOffIterator",
                ITERATOR_CONFIG, "securityLabel,public"));
    }

}
