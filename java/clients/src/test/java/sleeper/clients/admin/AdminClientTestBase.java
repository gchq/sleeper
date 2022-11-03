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
package sleeper.clients.admin;

import sleeper.ToStringPrintStream;
import sleeper.clients.AdminClient;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.TestConsoleInput;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ENCRYPTED;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class AdminClientTestBase {

    protected static final String MAIN_SCREEN = "\n" +
            "ADMINISTRATION COMMAND LINE CLIENT\n" +
            "----------------------------------\n" +
            "\n" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Print Sleeper instance property report\n" +
            "[2] Print Sleeper table names\n" +
            "[3] Print Sleeper table property report\n" +
            "[4] Update an instance or table property\n" +
            "\n";

    protected static final String EXIT_OPTION = "0";
    protected static final String INSTANCE_PROPERTY_REPORT_OPTION = "1";
    protected static final String TABLE_NAMES_REPORT_OPTION = "2";
    protected static final String TABLE_PROPERTY_REPORT_OPTION = "3";
    protected static final String UPDATE_PROPERTY_OPTION = "4";

    protected static final String PROMPT_INPUT_NOT_RECOGNISED = "Input not recognised please try again\n";

    protected static final String PROMPT_RETURN_TO_MAIN = "" +
            "\n\n----------------------------------\n" +
            "Hit enter to return to main screen\n";

    protected final AdminConfigStore store = mock(AdminConfigStore.class);
    protected final TestConsoleInput in = new TestConsoleInput();
    private final ToStringPrintStream out = new ToStringPrintStream();

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    protected static final String INSTANCE_ID = "test-instance";
    private static final String CONFIG_BUCKET_NAME = "sleeper-" + INSTANCE_ID + "-config";
    private static final String TABLE_NAME_VALUE = "test";

    protected String runClientGetOutput() throws IOException {
        AdminClient client = client();
        client.start(INSTANCE_ID);
        return out.toString();
    }

    private AdminClient client() throws UnsupportedEncodingException {
        return new AdminClient(store, out.consoleOut(), in.consoleIn());
    }

    protected void setInstanceProperties(InstanceProperties instanceProperties) {
        when(store.loadInstanceProperties(instanceProperties.get(ID))).thenReturn(instanceProperties);
    }

    protected void setInstanceProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        setInstanceProperties(instanceProperties);
        when(store.loadTableProperties(instanceProperties.get(ID), tableProperties.get(TABLE_NAME)))
                .thenReturn(tableProperties);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, String... tableNames) {
        setInstanceProperties(instanceProperties);
        when(store.listTables(instanceProperties.get(ID))).thenReturn(Arrays.asList(tableNames));
    }

    protected InstanceProperties createValidInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, INSTANCE_ID);
        instanceProperties.set(ACCOUNT, "1234567890");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(JARS_BUCKET, "bucket");
        instanceProperties.set(SUBNET, "subnet1");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        return instanceProperties;
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties) {
        return createValidTableProperties(instanceProperties, TABLE_NAME_VALUE);
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(ENCRYPTED, "false");
        return tableProperties;
    }
}
