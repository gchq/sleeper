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

package sleeper.systemtest.drivers.ingest;

import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestDeploymentContext;

import java.util.Map;
import java.util.TreeMap;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class IngestSourceContext {

    private final SystemTestDeploymentContext systemTest;
    private final SleeperInstanceContext instance;
    private final Map<String, String> filenameToPath = new TreeMap<>();
    private String bucketName;

    public IngestSourceContext(SystemTestDeploymentContext systemTest, SleeperInstanceContext instance) {
        this.systemTest = systemTest;
        this.instance = instance;
        bucketName = systemTest.getSystemTestBucketName();
    }

    public void useDataBucket() {
        bucketName = instance.getInstanceProperties().get(DATA_BUCKET);
    }

    public void useSystemTestBucket() {
        bucketName = systemTest.getSystemTestBucketName();
    }

    public String getSourceBucketName() {
        return bucketName;
    }

    public void reset() {
        useSystemTestBucket();
        filenameToPath.clear();
    }

    public void wroteFile(String name, String path) {
        filenameToPath.put(name, path);
    }

    public String getFilePath(String name) {
        String path = filenameToPath.get(name);
        if (path == null) {
            throw new IllegalStateException("Source file does not exist: " + name);
        }
        return path;
    }

    public String getBucketName() {
        return bucketName;
    }
}
