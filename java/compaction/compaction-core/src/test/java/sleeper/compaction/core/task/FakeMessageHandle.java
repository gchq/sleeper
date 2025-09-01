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
package sleeper.compaction.core.task;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.task.CompactionTask.MessageHandle;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class FakeMessageHandle implements MessageHandle {

    private final CompactionJob job;
    private final Consumer<CompactionJob> deleteFromQueue;
    private final Consumer<CompactionJob> returnToQueue;

    private FakeMessageHandle(CompactionJob job, Consumer<CompactionJob> deleteFromQueue, Consumer<CompactionJob> returnToQueue) {
        this.job = job;
        this.deleteFromQueue = deleteFromQueue;
        this.returnToQueue = returnToQueue;
    }

    public static FakeMessageHandle fromQueue(LinkedList<CompactionJob> queue) {
        return new FakeMessageHandle(queue.poll(),
                deleted -> {
                }, queue::addFirst);
    }

    public static FakeMessageHandle tracked(CompactionJob job, List<CompactionJob> consumedJobs, List<CompactionJob> jobsReturnedToQueue) {
        return new FakeMessageHandle(job, consumedJobs::add, jobsReturnedToQueue::add);
    }

    public CompactionJob getJob() {
        return job;
    }

    public void close() {
    }

    public void deleteFromQueue() {
        deleteFromQueue.accept(job);
    }

    public void returnToQueue() {
        returnToQueue.accept(job);
    }

}
