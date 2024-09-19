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

import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.validation.OptionalStack;

import java.util.ArrayList;
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
        addOptionalStack(stack(stackClass));
    }

    public <T> void removeOptionalStack(Class<T> stackClass) {
        removeOptionalStack(stack(stackClass));
    }

    public void addOptionalStack(OptionalStack stack) {
        LOGGER.info("Adding optional stack: {}", stack);
        updateOptionalStacks(stacks -> stacks.add(stack));
    }

    public void removeOptionalStack(OptionalStack stack) {
        LOGGER.info("Removing optional stack: {}", stack);
        updateOptionalStacks(stacks -> stacks.remove(stack));
    }

    private OptionalStack stack(Class<?> stackClass) {
        return EnumUtils.getEnumIgnoreCase(OptionalStack.class, stackClass.getSimpleName());
    }

    private void updateOptionalStacks(Consumer<Set<OptionalStack>> update) {
        InstanceProperties properties = instance.getInstanceProperties();
        Set<OptionalStack> optionalStacks = new LinkedHashSet<>(properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class));
        Set<OptionalStack> before = new LinkedHashSet<>(optionalStacks);
        update.accept(optionalStacks);
        if (before.equals(optionalStacks)) {
            LOGGER.info("Optional stacks unchanged, not redeploying");
            return;
        }
        properties.setEnumList(OPTIONAL_STACKS, new ArrayList<>(optionalStacks));
        instance.redeployCurrentInstance();
    }
}
