/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.query;

import org.junit.jupiter.api.Test;

public class QueryWebSocketIteratorTest {

    // When I iterate through the data and a successful result occurs, the rows are returned
    // When I iterate through the data and a failure occurs, the exception is thrown out of the iterator
    // When I iterate through the data without any results or failure, it times out with a configurable wait time (set to zero or close to zero for the test)
    // When I close the iterator the web socket should close

    @Test
    void shouldReturnRows() {

    }
}
