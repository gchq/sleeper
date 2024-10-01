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
package sleeper.query.runner.output;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
import sleeper.core.record.serialiser.JSONResultsBatchSerialiser;
import sleeper.core.schema.Schema;
import sleeper.query.model.QueryOrLeafPartitionQuery;
import sleeper.query.output.ResultsOutput;
import sleeper.query.output.ResultsOutputInfo;
import sleeper.query.output.ResultsOutputLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE;

/**
 * A query results output that writes results to an SQS queue.
 */
public class SQSResultsOutput implements ResultsOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQSResultsOutput.class);

    public static final String SQS = "SQS";
    public static final String SQS_RESULTS_URL = "sqsResultsUrl";
    public static final String BATCH_SIZE = "batchSize";
    private final AmazonSQS amazonSQS;
    private final Schema schema;
    private String sqsUrl;
    private final int batchSize;
    private final ResultsOutputLocation outputLocation;

    public SQSResultsOutput(InstanceProperties instanceProperties, AmazonSQS amazonSQS, Schema schema, Map<String, String> config) {
        this.amazonSQS = amazonSQS;
        this.schema = schema;
        this.sqsUrl = config.get(SQS_RESULTS_URL);
        if (null == this.sqsUrl) {
            this.sqsUrl = instanceProperties.get(QUERY_RESULTS_QUEUE_URL);
        }
        if (null == this.sqsUrl) {
            throw new IllegalArgumentException("Queue to output results to cannot be found in either the config or the instance properties");
        }
        this.outputLocation = new ResultsOutputLocation("sqs", this.sqsUrl);
        this.batchSize = null != config.get(BATCH_SIZE) ? Integer.parseInt(config.get(BATCH_SIZE)) : instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);
    }

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Record> results) {
        String queryId = query.getQueryId();
        long count = 0;
        try {
            if (!results.hasNext()) {
                sendBatchToSQS(new ResultsBatch(queryId, schema, List.of()), 0, sqsUrl);
            } else {
                List<Record> batch = new ArrayList<>();
                int size = 0;
                int batchNumber = 1;
                while (results.hasNext()) {
                    batch.add(results.next());
                    size++;
                    if (size >= batchSize) {
                        sendBatchToSQS(new ResultsBatch(queryId, schema, batch), batchNumber, sqsUrl);
                        batch.clear();
                        count += size;
                        size = 0;
                        batchNumber++;
                    }
                }
                if (!batch.isEmpty()) {
                    sendBatchToSQS(new ResultsBatch(queryId, schema, batch), batchNumber, sqsUrl);
                    count += batch.size();
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception sending results to SQS", e);
            return new ResultsOutputInfo(count, Collections.singletonList(this.outputLocation), e);
        } finally {
            try {
                results.close();
            } catch (Exception e) {
                LOGGER.error("Exception closing results of query", e);
            }
        }

        return new ResultsOutputInfo(count, Collections.singletonList(this.outputLocation));
    }

    private void sendBatchToSQS(ResultsBatch resultsBatch, int batchNumber, String sqsUrl) {
        sendResultsToSQS(resultsBatch, sqsUrl);
        LOGGER.info("Sent " + resultsBatch.getRecords().size() + " records to SQS (batch number " + batchNumber + ")");
    }

    private void sendResultsToSQS(ResultsBatch resultsBatch, String sqsUrl) {
        String serialisedResults = new JSONResultsBatchSerialiser().serialise(resultsBatch);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(sqsUrl)
                .withMessageBody(serialisedResults);
        amazonSQS.sendMessage(sendMessageRequest);
    }
}
