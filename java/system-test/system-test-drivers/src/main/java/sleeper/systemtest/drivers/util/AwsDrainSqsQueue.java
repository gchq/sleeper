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
package sleeper.systemtest.drivers.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toMap;
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

    private final ReceiveMessages receiveMessages;
    private final int numThreads;
    private final int messagesPerBatchPerThread;
    private final int messagesPerReceive;
    private final int waitTimeSeconds;

    private AwsDrainSqsQueue(Builder builder) {
        receiveMessages = builder.receiveMessages;
        messagesPerBatchPerThread = builder.messagesPerBatchPerThread;
        messagesPerReceive = builder.messagesPerReceive;
        numThreads = builder.numThreads;
        waitTimeSeconds = builder.waitTimeSeconds;
    }

    public static AwsDrainSqsQueue forSystemTests(SqsClient sqsClient) {
        return withClient(sqsClient)
                .numThreads(10)
                .messagesPerBatchPerThread(10_000)
                .waitTimeSeconds(20) // Note that this is the maximum wait time
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
        return builder().sqsClient(sqsClient);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Stream<Message> drain(String queueUrl) {
        LOGGER.info("Draining queue until empty: {}", queueUrl);
        return Stream.iterate(
                receiveMessageBatch(queueUrl), not(List::isEmpty), lastJobs -> receiveMessageBatch(queueUrl))
                .flatMap(List::stream);
    }

    public Stream<Message> drainExpectingMessages(int expectedMessages, String queueUrl) {
        LOGGER.info("Draining queue until empty, expecting {} messages: {}", queueUrl);
        return Stream.iterate(
                ReceiveBatchResult.first(receiveMessageBatch(queueUrl)),
                lastResult -> lastResult.hasMessages() || lastResult.totalMessages() < expectedMessages,
                lastResult -> lastResult.receiveNext(() -> receiveMessageBatch(queueUrl)))
                .flatMap(ReceiveBatchResult::stream);
    }

    public EmptyQueueResults empty(List<String> queueUrls) {
        LOGGER.info("Emptying queues: {}", queueUrls);
        EmptyQueueResults results = Stream.iterate(
                emptyMessageBatch(queueUrls),
                EmptyQueueResults::isAnyMessagesDeleted,
                lastResults -> emptyMessageBatch(lastResults.getQueueUrlsWithMessages()))
                .reduce(EmptyQueueResults.none(queueUrls),
                        (results1, results2) -> EmptyQueueResults.combine(Stream.of(results1, results2)));
        LOGGER.info("Deleted messages from queues: {}", results);
        return results;
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

    private EmptyQueueResults emptyMessageBatch(List<String> queueUrls) {
        List<List<String>> threadQueueBuckets = bucketQueuesByThreads(queueUrls);
        List<Callable<EmptyQueueResults>> callables = threadQueueBuckets.stream()
                .map(threadQueueUrls -> (Callable<EmptyQueueResults>) () -> emptyMessageBatchBucket(threadQueueUrls))
                .toList();
        return EmptyQueueResults.combine(streamInvokeAll(callables));
    }

    private List<List<String>> bucketQueuesByThreads(List<String> queueUrls) {
        int numBuckets = Math.min(numThreads, queueUrls.size());
        List<List<String>> threadBuckets = IntStream.range(0, numBuckets)
                .mapToObj(i -> (List<String>) new ArrayList<String>())
                .toList();
        Queue<String> remaining = new ArrayDeque<>(queueUrls);
        int thread = 0;
        for (String queueUrl = remaining.poll(); queueUrl != null; queueUrl = remaining.poll()) {
            threadBuckets.get(thread).add(queueUrl);
            thread = (thread + 1) % numBuckets;
        }
        return threadBuckets;
    }

    private EmptyQueueResults emptyMessageBatchBucket(List<String> queueUrls) {
        return EmptyQueueResults.fromOneThreadBatch(queueUrls.stream()
                .map(queueUrl -> new EmptyQueueResult(queueUrl, receiveMessageBatchOneThread(queueUrl, counting()))));
    }

    private <A, R> Stream<R> receiveOnThreads(String queueUrl, Collector<Message, A, R> threadCollector) {
        return streamInvokeAll(IntStream.range(0, numThreads)
                .mapToObj(i -> (Callable<R>) () -> receiveMessageBatchOneThread(queueUrl, threadCollector))
                .toList());
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
                ReceiveBatchResult.first(receiveMessages(queueUrl)),
                lastResult -> lastResult.hasMessages(),
                lastResult -> lastResult.totalMessages() < messagesPerBatchPerThread
                        ? lastResult.receiveNext(() -> receiveMessages(queueUrl))
                        : lastResult.noMessagesNext())
                .flatMap(ReceiveBatchResult::stream)
                .collect(collector);
    }

    private List<Message> receiveMessages(String queueUrl) {
        return receiveMessages.receiveAndDeleteMessages(queueUrl, messagesPerReceive, waitTimeSeconds);
    }

    private record ReceiveBatchResult(List<Message> messages, int totalMessages, int totalReceives) {

        static ReceiveBatchResult first(List<Message> messages) {
            return new ReceiveBatchResult(messages, messages.size(), 1);
        }

        boolean hasMessages() {
            return !messages.isEmpty();
        }

        ReceiveBatchResult receiveNext(Supplier<List<Message>> receiveMessages) {
            List<Message> receivedMessages = receiveMessages.get();
            int newTotal = totalMessages + receivedMessages.size();
            LOGGER.info("Recevied {} messages, total of {} for batch", receivedMessages.size(), newTotal);
            return new ReceiveBatchResult(receivedMessages, newTotal, totalReceives + 1);
        }

        ReceiveBatchResult noMessagesNext() {
            return new ReceiveBatchResult(List.of(), totalMessages, totalReceives);
        }

        Stream<Message> stream() {
            return messages.stream();
        }
    }

    public record EmptyQueueResult(String queueUrl, long messages) {
    }

    public static class EmptyQueueResults {
        private final Map<String, Long> messagesByQueueUrl;

        private EmptyQueueResults(Map<String, Long> messagesByQueueUrl) {
            this.messagesByQueueUrl = messagesByQueueUrl;
        }

        private static EmptyQueueResults none(List<String> queueUrls) {
            return new EmptyQueueResults(queueUrls.stream()
                    .collect(toMap(queueUrl -> queueUrl, queueUrl -> 0L)));
        }

        private static EmptyQueueResults fromOneThreadBatch(Stream<EmptyQueueResult> results) {
            return new EmptyQueueResults(
                    results.collect(toMap(EmptyQueueResult::queueUrl, EmptyQueueResult::messages)));
        }

        private static EmptyQueueResults combine(Stream<EmptyQueueResults> results) {
            return new EmptyQueueResults(
                    results.flatMap(EmptyQueueResults::stream)
                            .collect(groupingBy(EmptyQueueResult::queueUrl,
                                    summingLong(EmptyQueueResult::messages))));
        }

        public boolean isAnyMessagesDeleted() {
            return messagesByQueueUrl.values().stream().anyMatch(count -> count > 0);
        }

        public List<String> getQueueUrlsWithMessages() {
            return stream()
                    .filter(result -> result.messages() > 0)
                    .map(EmptyQueueResult::queueUrl)
                    .toList();
        }

        public Stream<EmptyQueueResult> stream() {
            return messagesByQueueUrl.entrySet().stream()
                    .map(entry -> new EmptyQueueResult(entry.getKey(), entry.getValue()));
        }

        public long getMessagesDeleted(String queueUrl) {
            return messagesByQueueUrl.getOrDefault(queueUrl, 0L);
        }

        @Override
        public String toString() {
            return messagesByQueueUrl.toString();
        }
    }

    public static class Builder {

        private ReceiveMessages receiveMessages;
        private int numThreads = 1;
        private int messagesPerBatchPerThread = 10_000;
        private int messagesPerReceive = 10;
        private int waitTimeSeconds = 10;

        private Builder() {
        }

        public Builder sqsClient(SqsClient sqsClient) {
            return receiveMessages(ReceiveMessages.from(sqsClient));
        }

        public Builder receiveMessages(ReceiveMessages receiveMessages) {
            this.receiveMessages = receiveMessages;
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

        public Builder messagesPerReceive(int messagesPerReceive) {
            this.messagesPerReceive = messagesPerReceive;
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
