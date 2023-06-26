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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.Executor;
import sleeper.bulkimport.starter.executor.ExecutorFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.utils.HadoopPathUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.utils.HadoopPathUtils.streamFiles;

/**
 * The {@link BulkImportStarterLambda} consumes {@link sleeper.bulkimport.job.BulkImportJob} messages from SQS and starts executes them using
 * an {@link Executor}.
 */
public class BulkImportStarterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportStarterLambda.class);

    private final Executor executor;
    private final Configuration hadoopConfig;
    private final BulkImportJobSerDe bulkImportJobSerDe = new BulkImportJobSerDe();
    private final InstanceProperties instanceProperties;

    public BulkImportStarterLambda() throws IOException {
        this(AmazonS3ClientBuilder.defaultClient(),
                AmazonElasticMapReduceClientBuilder.defaultClient(),
                AWSStepFunctionsClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient());
    }

    public BulkImportStarterLambda(AmazonS3 s3Client, AmazonElasticMapReduce emrClient,
                                   AWSStepFunctions stepFunctionsClient, AmazonDynamoDB dynamoDB) throws IOException {
        this(loadInstanceProperties(s3Client), s3Client, emrClient, stepFunctionsClient, dynamoDB);
    }

    public BulkImportStarterLambda(InstanceProperties properties, AmazonS3 s3Client, AmazonElasticMapReduce emrClient,
                                   AWSStepFunctions stepFunctionsClient, AmazonDynamoDB dynamoDB) {
        this(new ExecutorFactory(properties, s3Client, emrClient, stepFunctionsClient, dynamoDB).createExecutor(),
                properties);
    }

    public BulkImportStarterLambda(Executor executor, InstanceProperties properties) {
        this(executor, properties, new Configuration());
    }

    public BulkImportStarterLambda(Executor executor, InstanceProperties properties, Configuration hadoopConfig) {
        this.executor = executor;
        this.instanceProperties = properties;
        this.hadoopConfig = hadoopConfig;
    }

    private static InstanceProperties loadInstanceProperties(AmazonS3 s3Client) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
        return instanceProperties;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        LOGGER.info("Received request: {}", event);
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .map(bulkImportJobSerDe::fromJson)
                .map(this::expandDirectories)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(executor::runJob);
        return null;
    }

    private Optional<BulkImportJob> expandDirectories(BulkImportJob job) {
        BulkImportJob.Builder builder = job.toBuilder();
        List<String> files = new ArrayList<>();
        try {
            streamFiles(job.getFiles(), hadoopConfig, instanceProperties.get(FILE_SYSTEM))
                    .map(HadoopPathUtils::getRequestPath)
                    .forEach(files::add);
        } catch (UncheckedIOException e) {
            LOGGER.warn("Could not expand directories for job: {}", job);
            return Optional.empty();
        }
        return Optional.of(builder.files(files).build());
    }
}
