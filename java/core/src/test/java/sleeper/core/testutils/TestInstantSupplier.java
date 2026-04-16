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
package sleeper.core.testutils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Wrapper class around the Supplier Instant interface to be able to assert cleanly there are no remaining expected
 * time calls.
 */
public class TestInstantSupplier implements Supplier<Instant> {

    private final List<Instant> remainingTimes;

    public TestInstantSupplier(List<Instant> initialTimes) {
        this.remainingTimes = new ArrayList<>(initialTimes);
    }

    @Override
    public Instant get() {
        return remainingTimes.remove(0);
    }

    public List<Instant> getRemainingTimes() {
        return remainingTimes;
    }

}
