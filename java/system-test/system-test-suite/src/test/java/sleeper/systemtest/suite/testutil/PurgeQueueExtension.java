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

package sleeper.systemtest.suite.testutil;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

public class PurgeQueueExtension implements AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(PurgeQueueExtension.class);
    private final List<String> queueUrls;
    private final Consumer<String> purgeQueue;
    private final Runnable waitFn;

    public PurgeQueueExtension(List<String> queueUrls, AmazonSQS sqsClient) {
        this(queueUrls, queueUrl -> sqsClient.purgeQueue(new PurgeQueueRequest().withQueueUrl(queueUrl)), () -> {
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public PurgeQueueExtension(List<String> queueUrls, Consumer<String> purgeQueue, Runnable waitFn) {
        this.queueUrls = queueUrls;
        this.purgeQueue = purgeQueue;
        this.waitFn = waitFn;
    }

    @Override
    public void afterEach(ExtensionContext testContext) {
        if (testContext.getExecutionException().isPresent()) {
            afterTestFailed();
        } else {
            afterTestPassed();
        }
    }

    public void afterTestFailed() {
        LOGGER.info("Test failed, purging queues: {}", queueUrls);
        purgeQueueAndWait();
    }

    public void afterTestPassed() {
        LOGGER.info("Test passed, not purging queues");
    }

    private void purgeQueueAndWait() {
        for (String queueUrl : queueUrls) {
            purgeQueue.accept(queueUrl);
            LOGGER.info("Waiting 60s for queue {} to purge", queueUrl);
            waitFn.run();
        }
    }
}
