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

package sleeper.configuration.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstancePropertyGroup;

import java.util.List;

/**
 * Definitions of instance properties relating to bulk import on AWS EKS.
 */
public interface EKSProperty {
    UserDefinedInstanceProperty BULK_IMPORT_REPO = Index.propertyBuilder("sleeper.bulk.import.eks.repo")
            .description("(EKS mode only) The name of the ECS repository where the Docker image for the bulk import container is stored.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty EKS_CLUSTER_ADMIN_ROLES = Index.propertyBuilder("sleeper.bulk.import.eks.cluster.admin.roles")
            .description("(EKS mode only) Names of AWS IAM roles which should have access to administer the EKS cluster.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty EKS_IS_NATIVE_LIBS_IMAGE = Index.propertyBuilder("sleeper.bulk.import.eks.is.native.libs.image")
            .description("(EKS mode only) Set to true if sleeper.bulk.import.eks.repo contains the image built with " +
                    "native Hadoop libraries. By default when deploying with the EKS stack enabled, an image will be " +
                    "built based on the official Spark Docker image, so this should be false.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("false")
            .runCdkDeployWhenChanged(true).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
