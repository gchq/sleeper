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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;

import java.util.List;

/**
 * Definitions of instance properties relating to ingest backed by an Arrow buffer.
 */
public interface ArrowIngestProperty {
    UserDefinedInstanceProperty ARROW_INGEST_WORKING_BUFFER_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.working.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow working buffer. This buffer is used for sorting and other sundry " +
                    "activities. " +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [256MB]")
            .defaultValue("268435456")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_BATCH_BUFFER_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.batch.buffer.bytes")
            .description("The number of bytes to allocate to the Arrow batch buffer, which is used to hold the records before they are " +
                    "written to local disk. A larger value means that the local disk holds fewer, larger files, which are more efficient " +
                    "to merge together during an upload to S3. Larger values may require a larger working buffer. " +
                    "Note that this is off-heap memory, which is in addition to the memory assigned to the JVM.\n" +
                    "(arrow-based ingest only) [1GB]")
            .defaultValue("1073741824")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_LOCAL_STORE_BYTES = Index.propertyBuilder("sleeper.ingest.arrow.max.local.store.bytes")
            .description("The maximum number of bytes to store on the local disk before uploading to the main Sleeper store. A larger value " +
                    "reduces the number of S3 PUTs that are required to upload thle data to S3 and results in fewer files per partition.\n" +
                    "(arrow-based ingest only) [2GB]")
            .defaultValue("2147483648")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS = Index.propertyBuilder("sleeper.ingest.arrow.max.single.write.to.file.records")
            .description("The number of records to write at once into an Arrow file in the local store. A single Arrow file contains many of " +
                    "these micro-batches and so this parameter does not significantly affect the final size of the Arrow file. " +
                    "Larger values may require a larger working buffer.\n" +
                    "(arrow-based ingest only) [1K]")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
