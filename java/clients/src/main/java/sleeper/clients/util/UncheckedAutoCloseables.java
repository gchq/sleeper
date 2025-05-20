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

import java.util.List;

/**
 * Handles shut down of multiple closeables.
 */
public class UncheckedAutoCloseables implements UncheckedAutoCloseable {
    private final List<UncheckedAutoCloseable> closeables;

    public UncheckedAutoCloseables(List<UncheckedAutoCloseable> closeables) {
        this.closeables = closeables;
    }

    @Override
    public void close() {
        for (UncheckedAutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

}
