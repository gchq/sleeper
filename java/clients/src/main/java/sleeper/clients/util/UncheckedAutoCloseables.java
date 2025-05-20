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
package sleeper.clients.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles shut down of multiple closeables.
 */
public class UncheckedAutoCloseables implements UncheckedAutoCloseable {
    public static final Logger LOGGER = LoggerFactory.getLogger(UncheckedAutoCloseables.class);

    private final List<UncheckedAutoCloseable> closeables;

    public UncheckedAutoCloseables(List<UncheckedAutoCloseable> closeables) {
        this.closeables = closeables;
    }

    @Override
    public void close() {
        List<RuntimeException> failures = new ArrayList<>();
        for (UncheckedAutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (RuntimeException e) {
                failures.add(e);
                LOGGER.error("Failed closing a resource", e);
            }
        }
        if (!failures.isEmpty()) {
            throw new FailedCloseException(failures);
        }
    }

}
