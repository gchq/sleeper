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
package sleeper.cdk.custom.testutil;

import com.amazonaws.services.lambda.runtime.Client;

public class FakeClient implements Client {

    @Override
    public String getInstallationId() {
        return "test-installation-id";
    }

    @Override
    public String getAppTitle() {
        return "test-app-title";
    }

    @Override
    public String getAppVersionName() {
        return "test-app-version";
    }

    @Override
    public String getAppVersionCode() {
        return "test-app-version-code";
    }

    @Override
    public String getAppPackageName() {
        return "test-app-package-name";
    }

}
