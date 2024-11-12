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
package sleeper.core.statestore.commit;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class StateStoreCommitRequestInS3UploaderTest {

    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final Map<String, String> objectsByBucketAndKey = new HashMap<>();

    @Test
    void shouldUploadFileWhenTooBig() {
        // Given
        String commitJson = "{\"type\":\"test\",\"tableId\":\"test-table\"}";
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        StateStoreCommitRequestInS3Uploader uploader = uploaderWithMaxLengthAndFilenames(10, "commit");

        // When
        String resultJson = uploader.uploadAndWrapIfTooBig("test-table", commitJson);

        // Then
        assertThat(new StateStoreCommitRequestInS3SerDe().fromJson(resultJson))
                .isEqualTo(new StateStoreCommitRequestInS3("test-table/statestore/commitrequests/commit.json"));
        assertThat(objectsByBucketAndKey)
                .isEqualTo(Map.of("test-bucket/test-table/statestore/commitrequests/commit.json", commitJson));
    }

    @Test
    void shouldNotUploadFileWhenMeetsMax() {
        // Given
        String commitJson = "{\"type\":\"test\",\"tableId\":\"test-table\"}";
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        StateStoreCommitRequestInS3Uploader uploader = uploaderWithMaxLengthAndFilenames(commitJson.length(), "commit");

        // When
        String resultJson = uploader.uploadAndWrapIfTooBig("test-table", commitJson);

        // Then
        assertThat(resultJson).isEqualTo(commitJson);
        assertThat(objectsByBucketAndKey).isEmpty();
    }

    private StateStoreCommitRequestInS3Uploader uploaderWithMaxLengthAndFilenames(int maxLength, String... filenames) {
        return new StateStoreCommitRequestInS3Uploader(instanceProperties, client(), maxLength, List.of(filenames).iterator()::next);
    }

    private StateStoreCommitRequestInS3Uploader.Client client() {
        return (bucket, key, content) -> {
            objectsByBucketAndKey.put(bucket + "/" + key, content);
        };
    }

}
