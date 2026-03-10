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
package sleeper.restapi;

public class SleeperRestTest {

    @BeforeEach
    void setUp() {
        // Create instance of rest API to interogate
    }

    @@Nested
    @DisplayName("getVersion Functional Tests")
    class GetVersionTests {

        @Test
        void shouldRetrieveSleeperVersion() {
            //Given
                //check Rest API instatiated/accessible, sleeper instance given set version number for test
                // Create post request for getVersion with correct json
                // Validate JSON?

            //When
                // Request sent to api to retreive version

            //Then
                // Validate that version number matches

        }

        @Test
        void shouldErrorGivenIncompleteJsonRequest() {
            // Given
                // Generate incorrect Json for getVersion request

            // When
                // Send incorrect Json to API

            //Then
                // Validate correct error message returned
        }

    }


}
