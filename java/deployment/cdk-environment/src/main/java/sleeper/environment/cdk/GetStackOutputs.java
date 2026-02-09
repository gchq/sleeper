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

package sleeper.environment.cdk;

import com.google.gson.Gson;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import sleeper.environment.cdk.outputs.StackOutputs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class GetStackOutputs {

    private GetStackOutputs() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String environmentId = args[0];
        Path outputFile = Path.of(args[1]);

        List<String> stacks = SleeperEnvironmentCdkApp.getStackNamesForEnvironment(environmentId);

        try (CloudFormationClient cloudFormation = CloudFormationClient.create()) {
            StackOutputs outputs = StackOutputs.load(cloudFormation, stacks);
            Path outputDir = outputFile.getParent();
            if (outputDir != null) {
                Files.createDirectories(outputDir);
            }
            Files.writeString(outputFile, new Gson().toJson(outputs.toMap()));
        }
    }
}
