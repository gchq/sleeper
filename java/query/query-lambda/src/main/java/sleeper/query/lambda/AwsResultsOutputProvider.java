/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.lambda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputProvider;
import sleeper.query.core.rowretrieval.UnknownResultsPublisherException;
import sleeper.query.runner.output.NoResultsOutput;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.output.WebSocketOutput;
import sleeper.query.runner.output.WebSocketResultsOutput;

import java.util.HashMap;
import java.util.Map;

import static sleeper.query.runner.output.NoResultsOutput.NO_RESULTS_OUTPUT;

/**
 * A provider to create outputs to send query results in AWS.
 */
public class AwsResultsOutputProvider implements ResultsOutputProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsResultsOutputProvider.class);

    private final InstanceProperties instanceProperties;
    private final TableHadoopConfigurationProvider hadoopProvider;
    private final SqsClient sqsClient;

    public AwsResultsOutputProvider(InstanceProperties instanceProperties, TableHadoopConfigurationProvider hadoopProvider, SqsClient sqsClient) {
        this.instanceProperties = instanceProperties;
        this.hadoopProvider = hadoopProvider;
        this.sqsClient = sqsClient;
    }

    @Override
    public ResultsOutput getResultsOutput(TableProperties tableProperties, LeafPartitionQuery query) {
        Map<String, String> resultsPublisherConfig = query.getProcessingConfig().getResultsPublisherConfig();
        if (null == resultsPublisherConfig || resultsPublisherConfig.isEmpty()) {
            return new S3ResultsOutput(instanceProperties, tableProperties, hadoopProvider.getConfiguration(tableProperties), new HashMap<>());
        }
        String destination = resultsPublisherConfig.get(ResultsOutput.DESTINATION);
        if (SQSResultsOutput.SQS.equals(destination)) {
            return new SQSResultsOutput(instanceProperties, sqsClient, tableProperties.getSchema(), resultsPublisherConfig);
        } else if (S3ResultsOutput.S3.equals(destination)) {
            return new S3ResultsOutput(instanceProperties, tableProperties, hadoopProvider.getConfiguration(tableProperties), resultsPublisherConfig);
        } else if (WebSocketOutput.DESTINATION_NAME.equals(destination)) {
            return new WebSocketResultsOutput(tableProperties.getSchema(), resultsPublisherConfig);
        } else if (NO_RESULTS_OUTPUT.equals(destination)) {
            return new NoResultsOutput();
        } else {
            LOGGER.error("Unknown results publisher config: {}", resultsPublisherConfig);
            throw new UnknownResultsPublisherException(destination);
        }
    }

}
