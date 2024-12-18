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

package sleeper.clients.admin.testutils;

import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskStatusStore;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTaskStatusStore;
import sleeper.task.common.QueueMessageCount;

import static org.mockito.Mockito.when;
import static sleeper.clients.admin.properties.UpdatePropertiesRequestTestHelper.noChanges;
import static sleeper.clients.admin.properties.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.task.common.InMemoryQueueMessageCounts.noQueues;

public class RunAdminClient {
    private final ToStringConsoleOutput out;
    private final TestConsoleInput in;
    private final AdminConfigStoreTestHarness harness;
    private final AdminClientProcessTrackerHolder trackers = new AdminClientProcessTrackerHolder();
    private final UpdatePropertiesWithTextEditor editor;
    private QueueMessageCount.Client queueClient = noQueues();

    RunAdminClient(ToStringConsoleOutput out, TestConsoleInput in,
            AdminConfigStoreTestHarness harness,
            UpdatePropertiesWithTextEditor editor) {
        this.out = out;
        this.in = in;
        this.harness = harness;
        this.editor = editor;
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
        harness.setInstanceProperties(before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties before, InstanceProperties after, PropertyGroup group) throws Exception {
        harness.setInstanceProperties(before);
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
        harness.setInstanceProperties(properties, before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient editFromStore(InstanceProperties properties,
            TableProperties before, TableProperties after, PropertyGroup group) throws Exception {
        harness.setInstanceProperties(properties, before);
        when(editor.openPropertiesFile(before, group))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties) throws Exception {
        harness.setInstanceProperties(properties);
        when(editor.openPropertiesFile(properties))
                .thenReturn(noChanges(properties));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties, PropertyGroup propertyGroup) throws Exception {
        harness.setInstanceProperties(properties);
        when(editor.openPropertiesFile(properties, propertyGroup))
                .thenReturn(noChanges(properties));
        return this;
    }

    public RunAdminClient viewInEditorFromStore(InstanceProperties properties, TableProperties tableProperties) throws Exception {
        harness.setInstanceProperties(properties, tableProperties);
        when(editor.openPropertiesFile(tableProperties))
                .thenReturn(noChanges(tableProperties));
        return this;
    }

    public String exitGetOutput() throws Exception {
        in.enterNextPrompt(EXIT_OPTION);
        return runGetOutput();
    }

    public String runGetOutput() throws Exception {
        harness.startClient(trackers, queueClient);
        return out.toString();
    }

    public RunAdminClient queueClient(QueueMessageCount.Client queueClient) {
        this.queueClient = queueClient;
        return this;
    }

    public RunAdminClient tracker(CompactionJobTracker store) {
        trackers.setTracker(harness.getInstanceId(), store);
        return this;
    }

    public RunAdminClient tracker(CompactionTaskStatusStore store) {
        trackers.setTracker(harness.getInstanceId(), store);
        return this;
    }

    public RunAdminClient tracker(IngestJobStatusStore store) {
        trackers.setTracker(harness.getInstanceId(), store);
        return this;
    }

    public RunAdminClient tracker(IngestTaskStatusStore store) {
        trackers.setTracker(harness.getInstanceId(), store);
        return this;
    }

    public RunAdminClient batcherStore(IngestBatcherStore store) {
        trackers.setBatcherStore(harness.getInstanceId(), store);
        return this;
    }

    public AdminClientTrackerFactory trackers() {
        return trackers;
    }
}
