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
package sleeper.compaction.core.task;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Receives progress callbacks from a running compaction, and holds the latest progress to be checked by another thread.
 * <p>
 * Callbacks may be made from any thread, including native threads, and the compaction may be blocked while a callback
 * runs. This only performs an atomic update and does no other work.
 */
public class CompactionProgressMonitor implements LongConsumer {

    private final Supplier<Instant> timeSupplier;
    private final AtomicReference<CompactionProgress> progress = new AtomicReference<>();

    public CompactionProgressMonitor(Supplier<Instant> timeSupplier) {
        this.timeSupplier = timeSupplier;
    }

    @Override
    public void accept(long rowsProcessed) {
        Instant time = timeSupplier.get();
        progress.updateAndGet(last -> last == null
                ? CompactionProgress.firstUpdate(rowsProcessed, time)
                : last.update(rowsProcessed, time));
    }

    /**
     * Retrieves the latest progress.
     *
     * @return the latest progress, or an empty optional if no progress update has been received yet
     */
    public Optional<CompactionProgress> getProgress() {
        return Optional.ofNullable(progress.get());
    }
}
