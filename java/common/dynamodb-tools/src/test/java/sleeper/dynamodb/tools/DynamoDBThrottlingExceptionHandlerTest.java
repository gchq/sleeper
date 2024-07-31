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
package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDBThrottlingExceptionHandlerTest {
    private final List<Exception> throttlingExceptions = new ArrayList<>();
    private final List<Exception> otherExceptions = new ArrayList<>();

    @Test
    void shouldHandleThrottlingException() {
        // Given
        AmazonDynamoDBException e = new AmazonDynamoDBException("Throttling exception");
        e.setErrorCode("ThrottlingException");

        // When
        exceptionHandler().handle(e);

        // Then
        assertThat(throttlingExceptions).containsExactly(e);
        assertThat(otherExceptions).isEmpty();
    }

    @Test
    void shouldHandleOtherException() {
        // Given
        Exception e = new RuntimeException("Test exception");

        // When
        exceptionHandler().handle(e);

        // Then
        assertThat(throttlingExceptions).isEmpty();
        assertThat(otherExceptions).containsExactly(e);
    }

    private DynamoDBThrottlingExceptionHandler exceptionHandler() {
        return new DynamoDBThrottlingExceptionHandler(throttlingExceptions::add, otherExceptions::add);
    }
}
