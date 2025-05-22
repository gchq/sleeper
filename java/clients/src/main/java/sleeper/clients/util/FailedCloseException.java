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

public class FailedCloseException extends RuntimeException {

    private final List<RuntimeException> failures;

    public FailedCloseException(List<RuntimeException> failures) {
        super("Failed to close " + failures.size() + " resource(s)", failures.get(0));
        this.failures = failures;
    }

    public List<RuntimeException> getFailures() {
        return failures;
    }

}
