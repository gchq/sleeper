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

package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.Objects;

public class UserDefinedInstancePropertyConstants {
    //TABLE_PROPERTIES("sleeper.table.properties",Objects::nonNull),
    public static final InstanceProperty TABLE_PROPERTIES = named("sleeper.table.properties")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty ID = named("sleeper.id")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty JARS_BUCKET = named("sleeper.jars.bucket")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty USER_JARS = named("sleeper.userjars").build();
    public static final InstanceProperty TAGS_FILE = named("sleeper.tags.file").build();
    public static final InstanceProperty TAGS = named("sleeper.tags").build();
    public static final InstanceProperty STACK_TAG_NAME = named("sleeper.stack.tag.name")
            .defaultValue("DeploymentStack").build();
    public static final InstanceProperty RETAIN_INFRA_AFTER_DESTROY = named("sleeper.retain.infra.after.destroy")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse).build();
    public static final InstanceProperty OPTIONAL_STACKS = named("sleeper.optional.stacks")
            .defaultValue("CompactionStack,GarbageCollectorStack,IngestStack,PartitionSplittingStack,QueryStack," +
                    "AthenaStack,EmrBulkImportStack,DashboardStack")
            .build();
    public static final InstanceProperty ACCOUNT = named("sleeper.account")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty REGION = named("sleeper.region")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty VERSION = named("sleeper.version")
            .validationPredicate(Objects::nonNull).build();
    public static final InstanceProperty VPC_ID = named("sleeper.vpc")
            .validationPredicate(Objects::nonNull).build();

    private UserDefinedInstancePropertyConstants() {
        // Prevent instantiation
    }

    private static UserDefinedInstancePropertyImpl.Builder named(String name) {
        return UserDefinedInstancePropertyImpl.builder().propertyName(name);
    }

}
