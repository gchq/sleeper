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
package sleeper.clients.admin;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import sleeper.clients.admin.deploy.CdkDeployInstance;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.table.job.TableLister;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class AdminConfigStore {

    private final AmazonS3 s3;
    private final CdkDeployInstance cdk;
    private final Path generatedDirectory;

    public AdminConfigStore(AmazonS3 s3, CdkDeployInstance cdk, Path generatedDirectory) {
        this.s3 = s3;
        this.cdk = cdk;
        this.generatedDirectory = generatedDirectory;
    }

    public InstanceProperties loadInstanceProperties(String instanceId) {
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        } catch (IOException | AmazonS3Exception e) {
            throw new CouldNotLoadInstanceProperties(instanceId, e);
        }
        return instanceProperties;
    }

    public TableProperties loadTableProperties(String instanceId, String tableName) {
        return loadTableProperties(loadInstanceProperties(instanceId), tableName);
    }

    private TableProperties loadTableProperties(InstanceProperties instanceProperties, String tableName) {
        return new TablePropertiesProvider(s3, instanceProperties).getTableProperties(tableName);
    }

    public List<String> listTables(String instanceId) {
        return listTables(loadInstanceProperties(instanceId));
    }

    private List<String> listTables(InstanceProperties instanceProperties) {
        return new TableLister(s3, instanceProperties).listTables();
    }

    private Stream<TableProperties> streamTableProperties(InstanceProperties instanceProperties) {
        return listTables(instanceProperties).stream()
                .map(tableName -> loadTableProperties(instanceProperties, tableName));
    }

    public void updateInstanceProperty(String instanceId, UserDefinedInstanceProperty property, String propertyValue) {
        InstanceProperties properties = loadInstanceProperties(instanceId);
        properties.set(property, propertyValue);
        try {
            properties.saveToS3(s3);
        } catch (IOException | AmazonS3Exception e) {
            throw new CouldNotSaveInstanceProperties(instanceId, e);
        }
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, properties, streamTableProperties(properties));
    }

    public void updateTableProperty(String instanceId, String tableName, TableProperty property, String propertyValue) {
        TableProperties properties = loadTableProperties(instanceId, tableName);
        properties.set(property, propertyValue);
        try {
            properties.saveToS3(s3);
        } catch (IOException | AmazonS3Exception e) {
            throw new CouldNotSaveTableProperties(instanceId, tableName, e);
        }
    }

    public static class CouldNotLoadInstanceProperties extends RuntimeException {
        public CouldNotLoadInstanceProperties(String instanceId, Throwable e) {
            super("Could not load properties for instance " + instanceId, e);
        }
    }

    public static class CouldNotSaveInstanceProperties extends RuntimeException {
        public CouldNotSaveInstanceProperties(String instanceId, Throwable e) {
            super("Could not save properties for instance " + instanceId, e);
        }
    }

    public static class CouldNotSaveTableProperties extends RuntimeException {
        public CouldNotSaveTableProperties(String instanceId, String tableName, Throwable e) {
            super("Could not save properties for table " + tableName + " in instance " + instanceId, e);
        }
    }
}
