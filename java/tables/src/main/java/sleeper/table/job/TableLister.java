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
package sleeper.table.job;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class TableLister {
    private final AmazonS3 s3Client;
    private final InstanceProperties instanceProperties;

    public TableLister(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
    }

    public List<String> listTables() {
        ListObjectsV2Result result = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), TableProperties.TABLES_PREFIX + "/");
        List<S3ObjectSummary> objectSummaries = result.getObjectSummaries();
        return objectSummaries.stream()
                .map(S3ObjectSummary::getKey)
                .map(s -> s.substring(TableProperties.TABLES_PREFIX.length() + 1))
                .collect(Collectors.toList());
    }
}
