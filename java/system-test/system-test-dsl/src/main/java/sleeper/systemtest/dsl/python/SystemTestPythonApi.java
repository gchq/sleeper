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

package sleeper.systemtest.dsl.python;

import sleeper.systemtest.dsl.SystemTestContext;

import java.nio.file.Path;

public class SystemTestPythonApi {
    private final SystemTestContext context;

    public SystemTestPythonApi(SystemTestContext context) {
        this.context = context;
    }

    public SystemTestPythonIngest ingestByQueue() {
        return new SystemTestPythonIngest(context);
    }

    public SystemTestPythonBulkImport bulkImport() {
        return new SystemTestPythonBulkImport(context);
    }

    public SystemTestPythonQuery query(Path outputDir) {
        return new SystemTestPythonQuery(context, outputDir);
    }
}
