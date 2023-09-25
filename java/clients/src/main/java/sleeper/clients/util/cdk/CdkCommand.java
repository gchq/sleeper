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
package sleeper.clients.util.cdk;

import java.util.stream.Stream;

public interface CdkCommand {

    Stream<String> getCommand();

    Stream<String> getArguments();

    static CdkDeploy deployPropertiesChange() {
        return CdkDeploy.builder().ensureNewInstance(false).skipVersionCheck(false).deployPaused(false).build();
    }

    static CdkDeploy deploySystemTestStandalone() {
        return CdkDeploy.builder().ensureNewInstance(false).skipVersionCheck(false).deployPaused(false).build();
    }

    static CdkDeploy deployExisting() {
        return CdkDeploy.builder().ensureNewInstance(false).skipVersionCheck(true).deployPaused(false).build();
    }

    static CdkDeploy deployExistingPaused() {
        return CdkDeploy.builder().ensureNewInstance(false).skipVersionCheck(true).deployPaused(true).build();
    }

    static CdkDeploy deployNew() {
        return CdkDeploy.builder().ensureNewInstance(true).skipVersionCheck(false).deployPaused(false).build();
    }

    static CdkDeploy deployNewPaused() {
        return CdkDeploy.builder().ensureNewInstance(true).skipVersionCheck(false).deployPaused(true).build();
    }

    static CdkDestroy destroy() {
        return new CdkDestroy();
    }
}
