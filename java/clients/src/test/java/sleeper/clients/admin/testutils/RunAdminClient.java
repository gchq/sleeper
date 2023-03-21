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

import sleeper.ToStringPrintStream;
import sleeper.clients.AdminClient;
import sleeper.clients.admin.UpdatePropertiesWithNano;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.TestConsoleInput;

import static org.mockito.Mockito.when;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;

public class RunAdminClient {
    private final AdminClient client;
    private final ToStringPrintStream out;
    private final TestConsoleInput in;
    private final UpdatePropertiesWithNano editor;
    private final String instanceId;

    RunAdminClient(AdminClient client, ToStringPrintStream out, TestConsoleInput in,
                   UpdatePropertiesWithNano editor, String instanceId) {
        this.client = client;
        this.out = out;
        this.in = in;
        this.editor = editor;
        this.instanceId = instanceId;
    }

    public RunAdminClient specifyAllPrompts(boolean specifyAllPrompts) {
        in.specifyAllPrompts(specifyAllPrompts);
        return this;
    }

    public RunAdminClient enterPrompt(String entered) {
        in.enterNextPrompt(entered);
        return this;
    }

    public RunAdminClient enterPrompts(String... entered) {
        in.enterNextPrompts(entered);
        return this;
    }

    public RunAdminClient editAgain(InstanceProperties before, InstanceProperties after) throws Exception {
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return this;
    }

    public String exitGetOutput() throws Exception {
        in.enterNextPrompt(EXIT_OPTION);
        return runGetOutput();
    }

    public String runGetOutput() throws Exception {
        client.start(instanceId);
        return out.toString();
    }
}
