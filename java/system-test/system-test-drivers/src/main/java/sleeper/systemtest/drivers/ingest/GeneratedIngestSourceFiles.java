/*
 * Copyright 2022-2023 Crown Copyright
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

import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.stream.Collectors;

public class GeneratedIngestSourceFiles {

    private final String bucketName;
    private final List<S3Object> objects;

    public GeneratedIngestSourceFiles(String bucketName, List<S3Object> objects) {
        this.bucketName = bucketName;
        this.objects = objects;
    }

    public List<String> getJobIdsFromIndividualFiles() {
        return objects.stream().map(S3Object::key)
                .map(key -> key.substring("ingest/".length(), key.lastIndexOf('/')))
                .collect(Collectors.toUnmodifiableList());
    }

    public List<String> getIngestJobFilesCombiningAll() {
        return objects.stream().map(S3Object::key)
                .map(key -> bucketName + "/" + key)
                .collect(Collectors.toUnmodifiableList());
    }
}
