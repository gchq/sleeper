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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.ecr.AmazonECR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.NestedStack;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployExistingInstance;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class RedeployInstanceDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedeployInstanceDriver.class);

    private final SystemTestParameters parameters;
    private final SleeperInstanceContext instance;
    private final S3Client s3v2;
    private final AmazonECR ecr;

    public RedeployInstanceDriver(
            SystemTestParameters parameters, SleeperInstanceContext instance,
            S3Client s3v2, AmazonECR ecr) {
        this.parameters = parameters;
        this.instance = instance;
        this.s3v2 = s3v2;
        this.ecr = ecr;
    }

    public <T extends NestedStack> void addOptionalStack(Class<T> stackClass) throws InterruptedException {
        LOGGER.info("Adding optional stack: {}", stackClass);
        updateOptionalStacks(stacks -> stacks.add(stackClass.getSimpleName()));
    }

    public <T extends NestedStack> void removeOptionalStack(Class<T> stackClass) throws InterruptedException {
        LOGGER.info("Removing optional stack: {}", stackClass);
        updateOptionalStacks(stacks -> stacks.remove(stackClass.getSimpleName()));
    }

    private void updateOptionalStacks(Consumer<Set<String>> update) throws InterruptedException {
        InstanceProperties properties = instance.getInstanceProperties();
        Set<String> optionalStacks = new LinkedHashSet<>(properties.getList(OPTIONAL_STACKS));
        Set<String> before = new LinkedHashSet<>(optionalStacks);
        update.accept(optionalStacks);
        if (before.equals(optionalStacks)) {
            LOGGER.info("Optional stacks unchanged, not redeploying");
            return;
        }
        properties.set(OPTIONAL_STACKS, String.join(",", optionalStacks));
        redeploy();
    }

    private void redeploy() throws InterruptedException {
        try {
            DeployExistingInstance.builder()
                    .s3v2(s3v2).ecr(ecr)
                    .properties(instance.getInstanceProperties())
                    .tableProperties(instance.getTableProperties())
                    .scriptsDirectory(parameters.getScriptsDirectory())
                    .deployCommand(CdkCommand.deployExistingPaused())
                    .runCommand(ClientUtils::runCommandLogOutput)
                    .build().update();
            instance.reloadProperties();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
