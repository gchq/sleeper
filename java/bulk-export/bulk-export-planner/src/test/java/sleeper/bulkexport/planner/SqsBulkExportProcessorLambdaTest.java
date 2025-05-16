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
package sleeper.bulkexport.planner;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SqsBulkExportProcessorLambdaTest {

    private SqsBulkExportProcessor sqsBulkExportProcessor = mock(SqsBulkExportProcessor.class);
    private BulkExportQuerySerDe bulkExportQuerySerDe = new BulkExportQuerySerDe(); //mock(BulkExportQuerySerDe.class);

    private SqsBulkExportProcessorLambda sqsBulkExportProcessorLambda;
    private InstanceProperties instanceProperties;

    @BeforeEach
    public void setUp() throws ObjectFactoryException {
        instanceProperties = createTestInstanceProperties();
        sqsBulkExportProcessorLambda = new SqsBulkExportProcessorLambda(
                sqsBulkExportProcessor,
                bulkExportQuerySerDe,
                instanceProperties);
    }

    @Test
    public void testHandleRequestCatchesValidationException() {
        // Given: An SQS event with an invalid message
        SQSEvent event = createEvent("{\"rubbish\":\"feild\"}");

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessageContaining("tableId or tableName field must be provided");
    }

    @Test
    public void testHandleRequestCatchesRuntimeException() {
        // Given: An SQS event with a malformed message
        SQSEvent event = createEvent("{\"malformedJson\":}");

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("MalformedJsonException");
    }

    @Test
    public void testHandleRequestCatchesExceptionFromProcessor() throws ObjectFactoryException {
        // Given: An SQS event with a malformed message
        SQSEvent event = createEvent("{\"tableId\":\"testing\"}");
        doThrow(new ObjectFactoryException("Simulated Object Factory Exception"))
                .when(sqsBulkExportProcessor)
                .processExport(any(BulkExportQuery.class));

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(ObjectFactoryException.class)
                .hasMessageContaining("Simulated Object Factory Exception");
    }

    private SQSEvent createEvent(String messageBody) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(messageBody);
        event.setRecords(Collections.singletonList(message));
        return event;
    }
}
