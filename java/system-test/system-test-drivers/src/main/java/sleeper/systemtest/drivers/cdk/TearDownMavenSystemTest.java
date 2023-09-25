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

package sleeper.systemtest.drivers.cdk;

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.clients.teardown.RemoveECRRepositories;
import sleeper.clients.teardown.RemoveJarsBucket;
import sleeper.clients.teardown.TearDownInstance;
import sleeper.clients.teardown.WaitForStackToDelete;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.systemtest.drivers.instance.SystemTestParameters.buildJarsBucketName;
import static sleeper.systemtest.drivers.instance.SystemTestParameters.buildSystemTestECRRepoName;

public class TearDownMavenSystemTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownMavenSystemTest.class);

    private TearDownMavenSystemTest() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: <scripts-directory> <short-id> <instance-ids>");
        }
        Path scriptsDir = Path.of(args[0]);
        String shortId = args[1];
        List<String> instanceIds = List.of(args).subList(2, args.length);
        LOGGER.info("Found instance IDs to tear down: {}", instanceIds);
        for (String instanceId : instanceIds) {
            TearDownInstance.builder()
                    .scriptsDir(scriptsDir)
                    .instanceId(instanceId)
                    .tearDownWithDefaultClients();
        }

        try (CloudFormationClient cloudFormation = CloudFormationClient.create()) {
            LOGGER.info("Deleting system test CloudFormation stack");
            try {
                cloudFormation.deleteStack(builder -> builder.stackName(shortId));
            } catch (RuntimeException e) {
                LOGGER.warn("Failed deleting stack", e);
            }

            LOGGER.info("Waiting for system test CloudFormation stack to delete");
            WaitForStackToDelete.from(cloudFormation, shortId).pollUntilFinished();
        }

        try (S3Client s3 = S3Client.create()) {
            RemoveJarsBucket.remove(s3, buildJarsBucketName(shortId));
        }
        AmazonECR ecr = AmazonECRClientBuilder.defaultClient();
        RemoveECRRepositories.remove(ecr,
                PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId(shortId),
                List.of(buildSystemTestECRRepoName(shortId)));
    }
}
