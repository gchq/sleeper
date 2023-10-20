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

package sleeper.clients.teardown;

import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.StackStatus;
import software.amazon.awssdk.services.cloudformation.model.StackSummary;

import java.util.List;
import java.util.stream.Collectors;

public class CloudFormationStacks {
    private final List<String> stackNames;

    public CloudFormationStacks(CloudFormationClient cloudFormation) {
        this(cloudFormation.listStacksPaginator(b -> b.stackStatusFilters(
                        StackStatus.CREATE_COMPLETE, StackStatus.UPDATE_COMPLETE)).stackSummaries()
                .stream()
                .filter(stack -> stack.parentId() == null)
                .map(StackSummary::stackName).collect(Collectors.toList()));
    }

    public CloudFormationStacks(List<String> stackNames) {
        this.stackNames = stackNames;
    }

    public List<String> getStackNames() {
        return stackNames;
    }

    public boolean anyIn(String string) {
        return stackNames.stream().anyMatch(string::contains);
    }
}
