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

package sleeper.compaction.job.batcher;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperties.streamTablesFromS3;

public class TableBatcherLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableBatcherLambda.class);
    private AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tablePropertiesList;

    public TableBatcherLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());

        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(s3Client, s3Bucket);
        this.tablePropertiesList = streamTablesFromS3(s3Client, instanceProperties).collect(Collectors.toList());
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        LocalDateTime start = LocalDateTime.now();
        LOGGER.info("TableBatcherLambda lambda triggered at {}", event.getTime());

        new TableBatcher(instanceProperties, tablePropertiesList, new SQSTableBatcherQueueClient(sqsClient))
                .batchTables();

        LocalDateTime now = LocalDateTime.now();
        int durationInSeconds = (int) (Duration.between(start, now).toMillis() / 1000.0);
        LOGGER.info("TableBatcherLambda lambda finished at {} (ran for {} seconds)", LocalDateTime.now(), durationInSeconds);
    }
}
