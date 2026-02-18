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
package sleeper.cdk.custom.containers;

import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.LogEvent;
import com.google.cloud.tools.jib.event.EventHandlers;
import com.google.cloud.tools.jib.event.events.ProgressEvent;
import com.google.cloud.tools.jib.event.progress.ProgressEventHandler;
import com.google.cloud.tools.jib.event.progress.ProgressEventHandler.Update;
import org.slf4j.Logger;

import java.util.function.Consumer;

/**
 * Creates handlers for events emitted by Jib. Used when building or copying Docker images.
 */
public class JibEvents {

    private JibEvents() {
    }

    public static Containerizer logEvents(Logger logger, Containerizer containerizer) {
        return containerizer
                .addEventHandler(LogEvent.class, logEventHandler(logger))
                .addEventHandler(ProgressEvent.class, progressEventHandler(logger));
    }

    public static EventHandlers createEventHandlers(Logger logger) {
        return EventHandlers.builder()
                .add(LogEvent.class, logEventHandler(logger))
                .add(ProgressEvent.class, progressEventHandler(logger))
                .build();
    }

    public static Consumer<LogEvent> logEventHandler(Logger logger) {
        return log -> logger.info("From Jib: {}", log);
    }

    public static Consumer<ProgressEvent> progressEventHandler(Logger logger) {
        return new ProgressEventHandler(
                update -> logger.info("Jib progress {}, unfinished tasks: {}", progressToString(update), update.getUnfinishedLeafTasks()));
    }

    private static Object progressToString(Update update) {
        return new Object() {
            @Override
            public String toString() {
                return Math.round(update.getProgress() * 100.0) + "%";
            }
        };
    }

}
