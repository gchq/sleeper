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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SqsBulkExportProcessorLambdaTest {

    private SqsBulkExportProcessor sqsBulkExportProcessor = mock(SqsBulkExportProcessor.class);

    private SqsBulkExportProcessorLambda sqsBulkExportProcessorLambda;
    private BulkExportQuerySerDe bulkExportQuerySerDe = mock(BulkExportQuerySerDe.class);
    private AmazonS3Client s3Client;
    private AmazonSQSClient sqsClient;
    private AmazonDynamoDB dbClient;

    private InstanceProperties instanceProperties;

    @BeforeEach
    public void setUp() throws ObjectFactoryException {
        instanceProperties = createTestInstanceProperties();
        SendMessageResult result = new SendMessageResult();
        result.setMessageId("1234");

        when(sqsClient.sendMessage(anyString(), anyString())).thenReturn(result);
        sqsBulkExportProcessorLambda = new SqsBulkExportProcessorLambda(
                sqsBulkExportProcessor,
                bulkExportQuerySerDe,
                instanceProperties,
                sqsClient,
                s3Client,
                dbClient);
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testProcessingException() throws ObjectFactoryException {
        // Given: A valid SQS event with a message
        SQSEvent event = createEvent("{\"tableName\":\"testing\"}");

        when(bulkExportQuerySerDe.fromJson(anyString()))
                .thenReturn(createBulkExportQuery());

        // Mock the processor to throw an exception
        doThrow(new RuntimeException("Simulated processing failure"))
                .when(sqsBulkExportProcessor).processExport(any(BulkExportQuery.class));

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated processing failure");

        // Then: Verify the message was sent to the Dead Letter Queue
        //verify(sqsClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequestCatchesValidationException() {
        // Given: An SQS event with an invalid message
        SQSEvent event = createEvent("{\"tableName\":\"testing\",\"rubbish\":\"feild\"}");

        when(bulkExportQuerySerDe.fromJson(anyString()))
                .thenThrow(new BulkExportQueryValidationException("test-export", "Simulated invalid export query"));

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(BulkExportQueryValidationException.class)
                .hasMessageContaining("Simulated invalid export query");

        // Then: Verify the message was sent to the Dead Letter Queue
        //verify(sqsClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequestCatchesRuntimeException() {
        // Given: An SQS event with a malformed message
        SQSEvent event = createEvent("{\"malformedJson\":}");
        when(bulkExportQuerySerDe.fromJson(anyString()))
                .thenThrow(new RuntimeException("Simulated deserialization error"));

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Deserialization error");

        // Then: Verify the message was sent to the Dead Letter Queue
        //verify(sqsClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequestCatchesObjectFactoryException() throws ObjectFactoryException {
        // Given: An SQS event with a malformed message
        SQSEvent event = createEvent("{\"malformedJson\":}");
        when(bulkExportQuerySerDe.fromJson(anyString()))
                .thenReturn(createBulkExportQuery());
        doThrow(new ObjectFactoryException("Simulated Object Factory Exception"))
                .when(sqsBulkExportProcessor)
                .processExport(any(BulkExportQuery.class));

        // When: The handleRequest method is called
        // Then: Verify the exception was thrown
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(ObjectFactoryException.class)
                .hasMessageContaining("Simulated Object Factory Exception");

        // Then: Verify the message was sent to the Dead Letter Queue
        //verify(sqsClient).sendMessage(any(SendMessageRequest.class));
    }

    private BulkExportQuery createBulkExportQuery() {
        return BulkExportQuery.builder()
                .exportId("export-id")
                .tableId("table-id").build();
    }

    private SQSEvent createEvent(String messageBody) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(messageBody);
        event.setRecords(Collections.singletonList(message));
        return event;
    }
}