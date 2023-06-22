/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.bulkimport.starter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.Executor;
import sleeper.core.CommonTestConstants;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Testcontainers
public class BulkImportStarterIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    @Test
    public void shouldNotCreateAImportStarterWithoutConfig() {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonElasticMapReduce emrClient = mock(AmazonElasticMapReduce.class);
        AWSStepFunctions stepFunctionsClient = mock(AWSStepFunctions.class);
        AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);

        // When / Then
        assertThatThrownBy(() -> new BulkImportStarter(s3Client, emrClient, stepFunctionsClient, dynamoDB))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldHandleAValidRequest() {
        // Given
        Executor executor = mock(Executor.class);
        Context context = mock(Context.class);
        BulkImportStarter bulkImportStarter = new BulkImportStarter(executor);
        SQSEvent event = getSqsEvent();

        // When
        bulkImportStarter.handleRequest(event, context);

        // Then
        verify(executor, times(1)).runJob(any());
    }

    private SQSEvent getSqsEvent() {
        BulkImportJob importJob = new BulkImportJob.Builder().id("id").build();
        BulkImportJobSerDe jobSerDe = new BulkImportJobSerDe();
        String jsonQuery = jobSerDe.toJson(importJob);
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage sqsMessage = new SQSEvent.SQSMessage();
        sqsMessage.setBody(jsonQuery);
        event.setRecords(Lists.newArrayList(
                sqsMessage
        ));
        return event;
    }
}
