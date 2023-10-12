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
import sleeper.clients.admin.UpdatePropertiesWithTextEditor;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.job.common.QueueMessageCount;

import java.util.Collections;

import static org.mockito.Mockito.when;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.noChanges;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.job.common.QueueMessageCountsInMemory.noQueues;

public class RunAdminClient {
    private final ToStringPrintStream out;
    private final TestConsoleInput in;
    private final AdminConfigStoreTestHarness store;
    private final AdminClientStatusStoreHolder statusStores = new AdminClientStatusStoreHolder();
    private final UpdatePropertiesWithTextEditor editor;
    private QueueMessageCount.Client queueClient = noQueues();
    private final String instanceId;

    RunAdminClient(ToStringPrintStream out, TestConsoleInput in,
                   AdminConfigStoreTestHarness store,
                   UpdatePropertiesWithTextEditor editor,
                   String instanceId) {
        this.out = out;
        this.in = in;
        this.store = store;
        this.editor = editor;
        this.instanceId = instanceId;
    }

    public RunAdminClient enterPrompt(String entered) {
        in.enterNextPrompt(entered);
        return this;
    }

    public RunAdminClient enterPrompts(String... entered) {
        in.enterNextPrompts(entered);
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties before, InstanceProperties after) throws Exception {
        store.setInstanceProperties(before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties before, InstanceProperties after, PropertyGroup group) throws Exception {
        store.setInstanceProperties(before);
        when(editor.openPropertiesFile(before, group))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editAgain(InstanceProperties before, InstanceProperties after) throws Exception {
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties properties,
                                        TableProperties before, TableProperties after) throws Exception {
        store.setInstanceProperties(properties, before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties properties,
                                        TableProperties before, TableProperties after, PropertyGroup group)
            throws Exception {
        store.setInstanceProperties(properties, before);
        when(editor.openPropertiesFile(before, group))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties) throws Exception {
        store.setInstanceProperties(properties);
        when(editor.openPropertiesFile(properties))
                .thenReturn(noChanges(properties));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties, PropertyGroup propertyGroup) throws Exception {
        store.setInstanceProperties(properties);
        when(editor.openPropertiesFile(properties, propertyGroup))
                .thenReturn(noChanges(properties));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties, TableProperties tableProperties) throws Exception {
        store.setInstanceProperties(properties, tableProperties);
        when(editor.openPropertiesFile(tableProperties))
                .thenReturn(noChanges(tableProperties));
        return this;
    }

    public String exitGetOutput() throws Exception {
        in.enterNextPrompt(EXIT_OPTION);
        return runGetOutput();
    }

    public String runGetOutput() throws Exception {
        client().start(instanceId);
        return out.toString();
    }

    public RunAdminClient queueClient(QueueMessageCount.Client queueClient) {
        this.queueClient = queueClient;
        return this;
    }

    public RunAdminClient statusStore(CompactionJobStatusStore store) {
        statusStores.setStore(instanceId, store);
        return this;
    }

    public RunAdminClient statusStore(CompactionTaskStatusStore store) {
        statusStores.setStore(instanceId, store);
        return this;
    }

    public RunAdminClient statusStore(IngestJobStatusStore store) {
        statusStores.setStore(instanceId, store);
        return this;
    }

    public RunAdminClient statusStore(IngestTaskStatusStore store) {
        statusStores.setStore(instanceId, store);
        return this;
    }

    public RunAdminClient batcherStore(IngestBatcherStore store) {
        statusStores.setStore(instanceId, store);
        return this;
    }

    public AdminClientStatusStoreFactory statusStores() {
        return statusStores;
    }

    private AdminClient client() {
        return new AdminClient(store.getStore(), statusStores, editor, out.consoleOut(), in.consoleIn(),
                queueClient, (properties -> Collections.emptyMap()));
    }
}
