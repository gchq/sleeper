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
package sleeper.systemtest.ingest;

import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class InvokeIngestTaskCreation {

    private InvokeIngestTaskCreation() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        InvokeSystemTestLambda.forInstance(args[0], INGEST_LAMBDA_FUNCTION);
    }
}
