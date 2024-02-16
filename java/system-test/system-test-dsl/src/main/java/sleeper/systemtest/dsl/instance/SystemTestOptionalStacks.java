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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class SystemTestOptionalStacks {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestOptionalStacks.class);

    private final SystemTestInstanceContext instance;

    public SystemTestOptionalStacks(SystemTestInstanceContext instance) {
        this.instance = instance;
    }

    public <T> void addOptionalStack(Class<T> stackClass) {
        LOGGER.info("Adding optional stack: {}", stackClass);
        updateOptionalStacks(stacks -> stacks.add(stackClass.getSimpleName()));
    }

    public <T> void removeOptionalStack(Class<T> stackClass) {
        LOGGER.info("Removing optional stack: {}", stackClass);
        updateOptionalStacks(stacks -> stacks.remove(stackClass.getSimpleName()));
    }

    private void updateOptionalStacks(Consumer<Set<String>> update) {
        InstanceProperties properties = instance.getInstanceProperties();
        Set<String> optionalStacks = new LinkedHashSet<>(properties.getList(OPTIONAL_STACKS));
        Set<String> before = new LinkedHashSet<>(optionalStacks);
        update.accept(optionalStacks);
        if (before.equals(optionalStacks)) {
            LOGGER.info("Optional stacks unchanged, not redeploying");
            return;
        }
        properties.set(OPTIONAL_STACKS, String.join(",", optionalStacks));
        instance.redeployCurrentInstance();
    }
}
