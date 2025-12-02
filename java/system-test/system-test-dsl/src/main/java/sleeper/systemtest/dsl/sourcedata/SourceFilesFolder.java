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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class SourceFilesFolder {

    private final Supplier<String> bucketName;
    private final Supplier<String> folderName;

    public SourceFilesFolder(Supplier<String> bucketName, Supplier<String> folderName) {
        this.bucketName = bucketName;
        this.folderName = folderName;
    }

    public static SourceFilesFolder writeToSystemTestBucket(SystemTestContext context) {
        return writeToSystemTestBucket(context.systemTest(), context.sourceFiles().sourceBucketFolderName);
    }

    public static SourceFilesFolder writeToSystemTestBucket(DeployedSystemTestResources systemTest, String folderName) {
        return new SourceFilesFolder(systemTest::getSystemTestBucketName, () -> folderName);
    }

    public static SourceFilesFolder writeToDataBucket(SystemTestContext context) {
        return writeToDataBucket(context.instance());
    }

    public static SourceFilesFolder writeToDataBucket(SystemTestInstanceContext instance) {
        return new SourceFilesFolder(
                () -> instance.getInstanceProperties().get(DATA_BUCKET),
                () -> instance.getTableProperties().get(TABLE_ID));
    }

    public List<String> getIngestJobFilesInBucket(Stream<String> files) {
        return files.map(this::ingestJobFileInBucket).toList();
    }

    public String ingestJobFileInBucket(String filename) {
        return generateFilePathNoFs(filename);
    }

    String generateFilePathNoFs(String filename) {
        return bucketName.get() + "/" + folderName.get() + "/" + filename;
    }

    public String getBucketName() {
        return bucketName.get();
    }
}
