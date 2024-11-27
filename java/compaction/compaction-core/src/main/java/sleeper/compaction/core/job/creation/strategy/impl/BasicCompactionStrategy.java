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
package sleeper.compaction.core.job.creation.strategy.impl;

import sleeper.compaction.core.job.creation.strategy.DelegatingCompactionStrategy;

/**
 * A simple compaction strategy to compact all files over the batch size. Uses
 * {@link sleeper.compaction.core.job.creation.strategy.impl.BasicLeafStrategy}.
 */
public class BasicCompactionStrategy extends DelegatingCompactionStrategy {

    public BasicCompactionStrategy() {
        super(new BasicLeafStrategy());
    }

}
