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
package sleeper.systemtest.drivers.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.toUnmodifiableList;

public class AwsDrainSqsQueue {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsDrainSqsQueue.class);
    private static final ExecutorService EXECUTOR = createThreadPool();

    private static ExecutorService createThreadPool() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                40, 40,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private final SqsClient sqsClient;
    private final int numThreads;
    private final int messagesPerBatchPerThread;
    private final int waitTimeSeconds;

    private AwsDrainSqsQueue(Builder builder) {
        sqsClient = builder.sqsClient;
        messagesPerBatchPerThread = builder.messagesPerBatchPerThread;
        numThreads = builder.numThreads;
        waitTimeSeconds = builder.waitTimeSeconds;
    }

    public static AwsDrainSqsQueue forSystemTests(SqsClient sqsClient) {
        return withClient(sqsClient)
                .numThreads(10)
                .messagesPerBatchPerThread(10_000)
                .waitTimeSeconds(10)
                .build();
    }

    public static AwsDrainSqsQueue forLocalStackTests(SqsClient sqsClient) {
        return withClient(sqsClient)
                .numThreads(2)
                .messagesPerBatchPerThread(10)
                .waitTimeSeconds(1)
                .build();
    }

    private static Builder withClient(SqsClient sqsClient) {
        return new Builder().sqsClient(sqsClient);
    }

    public Stream<Message> drain(String queueUrl) {
        LOGGER.info("Draining queue until empty: {}", queueUrl);
        return Stream.iterate(
                receiveMessageBatch(queueUrl), not(List::isEmpty), lastJobs -> receiveMessageBatch(queueUrl))
                .flatMap(List::stream);
    }

    public void empty(List<String> queueUrls) {
        invokeAll(queueUrls.stream()
                .map(queueUrl -> (Runnable) () -> empty(queueUrl))
                .toList());
    }

    public void empty(String queueUrl) {
        LOGGER.info("Emptying queue: {}", queueUrl);
        long count = LongStream.iterate(
                emptyMessageBatch(queueUrl), n -> n > 0, lastCount -> emptyMessageBatch(queueUrl))
                .sum();
        LOGGER.info("Deleted {} messages from queue: {}", count, queueUrl);
    }

    private List<Message> receiveMessageBatch(String queueUrl) {
        List<Message> messages = receiveOnThreads(queueUrl, toUnmodifiableList())
                .flatMap(List::stream).toList();
        LOGGER.info("Received a batch of {} messages from queue: {}", messages.size(), queueUrl);
        return messages;
    }

    private long emptyMessageBatch(String queueUrl) {
        return receiveOnThreads(queueUrl, counting()).mapToLong(n -> n).sum();
    }

    private <A, R> Stream<R> receiveOnThreads(String queueUrl, Collector<Message, A, R> threadCollector) {
        return streamInvokeAll(IntStream.range(0, numThreads)
                .mapToObj(i -> (Callable<R>) () -> receiveMessageBatchOneThread(queueUrl, threadCollector))
                .toList());
    }

    private void invokeAll(List<Runnable> runnables) {
        List<Callable<Void>> callables = runnables.stream()
                .map(runnable -> (Callable<Void>) () -> {
                    runnable.run();
                    return null;
                }).toList();
        streamInvokeAll(callables)
                .forEach(result -> {
                });
    }

    private <R> Stream<R> streamInvokeAll(List<Callable<R>> callables) {
        try {
            List<Future<R>> results = EXECUTOR.invokeAll(callables);
            return results.stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private <A, R> R receiveMessageBatchOneThread(String queueUrl, Collector<Message, A, R> collector) {
        return Stream.iterate(
                receiveMessages(queueUrl), not(List::isEmpty), lastJobs -> receiveMessages(queueUrl))
                .limit(messagesPerBatchPerThread / 10)
                .flatMap(List::stream)
                .collect(collector);
    }

    public static Builder builder() {
        return new Builder();
    }

    private List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageResponse receiveResult = sqsClient.receiveMessage(request -> request
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(waitTimeSeconds));
        List<Message> messages = receiveResult.messages();
        if (messages.isEmpty()) {
            return List.of();
        }
        DeleteMessageBatchResponse deleteResult = sqsClient.deleteMessageBatch(request -> request
                .queueUrl(queueUrl)
                .entries(messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .toList()));
        if (!deleteResult.failed().isEmpty()) {
            throw new RuntimeException("Failed deleting compaction job messages: " + deleteResult.failed());
        }
        return messages;
    }

    public static class Builder {

        private SqsClient sqsClient;
        private int numThreads = 1;
        private int messagesPerBatchPerThread = 10_000;
        private int waitTimeSeconds = 10;

        private Builder() {
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder numThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder messagesPerBatchPerThread(int messagesPerBatchPerThread) {
            this.messagesPerBatchPerThread = messagesPerBatchPerThread;
            return this;
        }

        public Builder waitTimeSeconds(int waitTimeSeconds) {
            this.waitTimeSeconds = waitTimeSeconds;
            return this;
        }

        public AwsDrainSqsQueue build() {
            return new AwsDrainSqsQueue(this);
        }
    }
}
