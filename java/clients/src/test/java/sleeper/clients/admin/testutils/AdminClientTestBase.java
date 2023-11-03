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
package sleeper.clients.admin.testutils;

import sleeper.clients.AdminClient;
import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableIndex;
import sleeper.job.common.QueueMessageCount;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_CONFIGURATION_OPTION;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class AdminClientTestBase implements AdminConfigStoreTestHarness {

    protected final ToStringPrintStream out = new ToStringPrintStream();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    protected final UpdatePropertiesWithTextEditor editor = mock(UpdatePropertiesWithTextEditor.class);

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    protected String instanceId;
    protected static final String TABLE_NAME_VALUE = "test-table";

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    protected RunAdminClient runClient() {
        return new RunAdminClient(out, in, this, editor);
    }

    protected abstract TableIndex getTableIndex();

    protected abstract AdminClientPropertiesStore getStore();

    @Override
    public void startClient(AdminClientStatusStoreFactory statusStores, QueueMessageCount.Client queueClient)
            throws InterruptedException {
        new AdminClient(getTableIndex(), getStore(), statusStores,
                editor, out.consoleOut(), in.consoleIn(),
                queueClient, (properties -> Collections.emptyMap()))
                .start(instanceId);
    }

    protected InstanceProperties createValidInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        return instanceProperties;
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties) {
        return createValidTableProperties(instanceProperties, TABLE_NAME_VALUE);
    }

    protected TableProperties createValidTableProperties(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    protected RunAdminClient editInstanceConfiguration(InstanceProperties before, InstanceProperties after)
            throws Exception {
        return runClient().enterPrompt(INSTANCE_CONFIGURATION_OPTION)
                .editFromStore(before, after);
    }

    protected RunAdminClient viewInstanceConfiguration(InstanceProperties properties) throws Exception {
        return runClient().enterPrompt(INSTANCE_CONFIGURATION_OPTION)
                .viewInEditorFromStore(properties);
    }

    protected RunAdminClient editTableConfiguration(InstanceProperties instanceProperties,
                                                    TableProperties before, TableProperties after)
            throws Exception {
        return runClient()
                .enterPrompts(TABLE_CONFIGURATION_OPTION, before.get(TABLE_NAME))
                .editFromStore(instanceProperties, before, after);
    }

    protected RunAdminClient viewTableConfiguration(InstanceProperties instanceProperties,
                                                    TableProperties tableProperties)
            throws Exception {
        return runClient()
                .enterPrompts(TABLE_CONFIGURATION_OPTION, tableProperties.get(TABLE_NAME))
                .viewInEditorFromStore(instanceProperties, tableProperties);
    }
}
