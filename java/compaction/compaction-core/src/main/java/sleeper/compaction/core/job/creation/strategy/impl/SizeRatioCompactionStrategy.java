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
 * A compaction strategy that is similar to the strategy used in Accumulo.
 * Given files sorted into increasing size order, it tests whether the sum of the
 * sizes of the files excluding the largest is greater than or equal to a ratio
 * (3 by default) times the size of the largest. If this test is met then a
 * job is created from those files. If the test is not met then the largest file
 * is removed from the list of files and the test is repeated. This continues
 * until either a set of files matching the criteria is found, or there is only
 * one file left.
 * <p>
 * If this results in a group of files of size less than or equal to
 * sleeper.table.compaction.files.batch.size then a compaction job is created
 * for these files. Otherwise the files are split into batches and jobs are
 * created as long as the batch also meets the criteria.
 * <p>
 * The table property sleeper.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition
 * controls how many jobs can be running concurrently for each partition.
 */
public class SizeRatioCompactionStrategy extends DelegatingCompactionStrategy {

    public SizeRatioCompactionStrategy() {
        super(new SizeRatioLeafStrategy(), new SizeRatioShouldCreateJobsStrategy());
    }
}
