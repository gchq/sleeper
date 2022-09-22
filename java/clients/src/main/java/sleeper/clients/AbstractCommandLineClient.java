/*
 * Copyright 2022 Crown Copyright
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

package sleeper.clients;

import com.amazonaws.services.s3.AmazonS3;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.statestore.StateStoreException;
import sleeper.table.job.TableLister;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

public abstract class AbstractCommandLineClient {
    protected final AmazonS3 s3Client;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final InstanceProperties instanceProperties;
    protected AbstractCommandLineClient(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
    }

    public void run() throws StateStoreException {
        TableProperties tableProperties = getTableProperties();
        init(tableProperties);

        runQueries(tableProperties);
    }

    protected abstract void init(TableProperties tableProperties) throws StateStoreException;

    protected abstract void submitQuery(TableProperties tableProperties, Query query);

    private TableProperties getTableProperties() {
        String tableName = getTableName(s3Client, instanceProperties);
        if (tableName == null) {
            return null;
        }
        return tablePropertiesProvider.getTableProperties(tableName);
    }
    private String getTableName(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        List<String> tables = new TableLister(s3Client, instanceProperties).listTables();
        String tableName;
        if (tables.isEmpty()) {
            System.out.println("There are no tables. Please create one and add data before running this class.");
            return null;
        }
        if (tables.size() == 1) {
            tableName = tables.get(0);
            System.out.println("Querying table " + tableName);
        } else {
            Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
            while (true) {
                System.out.println("The system contains the following tables:");
                tables.forEach(System.out::println);
                System.out.println("Which table do you wish to query?");
                tableName = scanner.nextLine();
                if (tables.contains(tableName)) {
                    break;
                } else {
                    System.out.println("Invalid table, try again");
                }
            }
        }

        System.out.println("Thie table has schema " + tablePropertiesProvider.getTableProperties(tableName).getSchema());

        return tableName;
    }
    protected abstract void runQueries(TableProperties tableProperties);
    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }
}
