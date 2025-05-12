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

import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SqsBulkExportProcessorLambdaIT extends LocalStackTestBase {

    private InstanceProperties instanceProperties;
    private SqsBulkExportProcessorLambda sqsBulkExportProcessorLambda;

    @BeforeEach
    public void setUp() throws ObjectFactoryException {
        creatInstanceProperties();
        sqsBulkExportProcessorLambda = new SqsBulkExportProcessorLambda(sqsClient, s3Client, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    @Test
    public void testValidationExceptionThrowsException() {
        // Given
        SQSEvent event = createEvent("{\"invalidField\":\"value\"}");

        // When Then
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessageContaining("Query validation failed: tableId or tableName field must be provided");
    }

    private SQSEvent createEvent(String messageBody) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(messageBody);
        event.setRecords(Collections.singletonList(message));
        return event;
    }

    private void creatInstanceProperties() {
        instanceProperties = createTestInstanceProperties();
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
    }
}
