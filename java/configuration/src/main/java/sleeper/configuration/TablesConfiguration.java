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
package sleeper.configuration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class TablesConfiguration {
    private final List<TableProperties> tables;

    public TablesConfiguration(List<TableProperties> tables) {
        this.tables = tables;
    }

    public static TablesConfiguration loadFromS3(AmazonS3 s3, InstanceProperties instanceProperties) {
        return new TablesConfiguration(
                loadTablesFromS3(s3, instanceProperties).collect(Collectors.toList()));
    }

    public List<TableProperties> getTables() {
        return tables;
    }

    private static Stream<TableProperties> loadTablesFromS3(AmazonS3 s3, InstanceProperties instanceProperties) {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        return s3.listObjectsV2(configBucket, "tables/")
                .getObjectSummaries().stream()
                .map(tableConfigObject -> loadTableFromS3(s3, instanceProperties, tableConfigObject));
    }

    private static TableProperties loadTableFromS3(
            AmazonS3 s3, InstanceProperties instanceProperties, S3ObjectSummary tableConfigObject) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        try (InputStream in = s3.getObject(
                        tableConfigObject.getBucketName(),
                        tableConfigObject.getKey())
                .getObjectContent()) {
            tableProperties.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tableProperties;
    }
}
