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
package sleeper.clients.admin.testutils;

import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableStatus;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_CONFIGURATION_OPTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public abstract class AdminClientTestBase implements AdminConfigStoreTestHarness {

    protected final ToStringConsoleOutput out = new ToStringConsoleOutput();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    protected final UpdatePropertiesWithTextEditor editor = mock(UpdatePropertiesWithTextEditor.class);

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    protected InstanceProperties instanceProperties;
    protected String instanceId;
    protected String version = "1.2.3";
    protected String account = "test-account";
    protected String region = "test-region";
    protected static final String TABLE_NAME_VALUE = "test-table";

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
        this.instanceId = instanceProperties.get(ID);
        this.version = instanceProperties.get(VERSION);
        this.account = instanceProperties.get(ACCOUNT);
        this.region = instanceProperties.get(REGION);
    }

    protected RunAdminClient runClient() {
        return new RunAdminClient(out, in, this, editor);
    }

    protected InstanceProperties createValidInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        instanceProperties.unset(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS);
        return instanceProperties;
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties) {
        return createValidTableProperties(instanceProperties, TABLE_NAME_VALUE);
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties, TableStatus table) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, table.getTableName());
        tableProperties.set(TABLE_ID, table.getTableUniqueId());
        tableProperties.set(TABLE_ONLINE, Boolean.toString(table.isOnline()));
        return tableProperties;
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    protected RunAdminClient editInstanceConfiguration(InstanceProperties before, InstanceProperties after) throws Exception {
        return runClient().enterPrompt(INSTANCE_CONFIGURATION_OPTION)
                .editFromStore(before, after);
    }

    protected RunAdminClient viewInstanceConfiguration(InstanceProperties properties) throws Exception {
        return runClient().enterPrompt(INSTANCE_CONFIGURATION_OPTION)
                .viewInEditorFromStore(properties);
    }

    protected RunAdminClient editTableConfiguration(InstanceProperties instanceProperties,
            TableProperties before, TableProperties after) throws Exception {
        return runClient()
                .enterPrompts(TABLE_CONFIGURATION_OPTION, before.get(TABLE_NAME))
                .editFromStore(instanceProperties, before, after);
    }

    protected RunAdminClient viewTableConfiguration(InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        return runClient()
                .enterPrompts(TABLE_CONFIGURATION_OPTION, tableProperties.get(TABLE_NAME))
                .viewInEditorFromStore(instanceProperties, tableProperties);
    }
}
