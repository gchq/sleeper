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
package sleeper.compaction.core.job.dispatch;

import sleeper.compaction.core.job.CompactionJob;

import java.util.List;

public class CompactionJobDispatchCreationHelper {

    private CompactionJobDispatchCreationHelper() {
    }

    public static void populateBucketWithBatch(String key, List<CompactionJob> compactionJobList) {
        //FileReference file1 = factory.rootFile("file1.parquet", 123L);
        //FileReference file2 = factory.rootFile("file2.parquet", 456L);
    }

}
