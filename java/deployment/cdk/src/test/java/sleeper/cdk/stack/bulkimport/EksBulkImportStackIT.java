/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.stack.bulkimport;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.configuration.SparkConfigurationUtils;
import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.artefacts.containers.SleeperContainerImageDigestProvider;
import sleeper.cdk.artefacts.containers.SleeperContainerImagesFromProperties;
import sleeper.cdk.artefacts.jars.SleeperJarVersionIdProvider;
import sleeper.cdk.artefacts.jars.SleeperJarsFromProperties;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.testutil.SleeperStackTestBase;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.model.EksClusterType;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_IMAGE;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_AUTOMODE_CONFIGURE_NODEPOOL;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_AUTOMODE_FLUENT_BIT_LOGGING_ENABLED;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_AWSCLI_LAYER_ARN;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_CLUSTER_TYPE;
import static sleeper.core.properties.instance.EKSProperty.EKS_CLUSTER_ADMIN_ROLES;

public class EksBulkImportStackIT extends SleeperStackTestBase {

    @Test
    void shouldGenerateCloudFormationTemplate() {
        // Given
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import", ".json"));
    }

    @Test
    void shouldGenerateCloudFormationTemplateForAutomodeCluster() {
        // Given
        instanceProperties.setEnum(BULK_IMPORT_EKS_CLUSTER_TYPE, EksClusterType.AUTOMODE);
        instanceProperties.set(BULK_IMPORT_EKS_AUTOMODE_CONFIGURE_NODEPOOL, "true");
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import-automode", ".json"));
    }

    @Test
    void shouldGenerateCloudFormationTemplateForAutomodeClusterWithNodePoolConfigDisabled() {
        // Given
        instanceProperties.setEnum(BULK_IMPORT_EKS_CLUSTER_TYPE, EksClusterType.AUTOMODE);
        instanceProperties.set(BULK_IMPORT_EKS_AUTOMODE_CONFIGURE_NODEPOOL, "false");
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import-automode-nodepool-disabled", ".json"));
    }

    @Test
    void shouldGenerateCloudFormationTemplateForAutomodeClusterWithLoggingDisabled() {
        // Given
        instanceProperties.setEnum(BULK_IMPORT_EKS_CLUSTER_TYPE, EksClusterType.AUTOMODE);
        instanceProperties.set(BULK_IMPORT_EKS_AUTOMODE_CONFIGURE_NODEPOOL, "true");
        instanceProperties.set(BULK_IMPORT_EKS_AUTOMODE_FLUENT_BIT_LOGGING_ENABLED, "false");
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import-automode-logging-disabled", ".json"));
    }

    @Test
    void shouldGenerateCloudFormationTemplateWithAdminRoles() {
        // Given
        instanceProperties.set(EKS_CLUSTER_ADMIN_ROLES, "admin-role-one,admin-role-two");
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import-admin-roles", ".json"));
    }

    @Test
    void shouldGenerateCloudFormationTemplateWithAwsCliLayer() {
        // Given
        instanceProperties.set(BULK_IMPORT_EKS_AWSCLI_LAYER_ARN,
                "arn:aws:lambda:test-region:123456789012:layer:AWSCLI:1");
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import-awscli-layer", ".json"));
    }

    @Test
    void shouldUseCustomImageForFargateCluster() {
        assertUsesCustomImage(EksClusterType.FARGATE);
    }

    @Test
    void shouldUseCustomImageForAutoModeCluster() {
        assertUsesCustomImage(EksClusterType.AUTOMODE);
    }

    private void assertUsesCustomImage(EksClusterType clusterType) {
        // Given
        String customImage = "registry.example.com/custom/spark@sha256:" + "a".repeat(64);
        instanceProperties.setEnum(BULK_IMPORT_EKS_CLUSTER_TYPE, clusterType);
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);
        SleeperContainerImagesFromProperties images = new SleeperContainerImagesFromProperties(instanceProperties,
                new SleeperContainerImageDigestProvider((image, repository) -> "test-digest")) {
            @Override
            public String getDockerImageName(DockerDeployment deployment) {
                assertThat(deployment).isSameAs(DockerDeployment.EKS_BULK_IMPORT);
                return customImage;
            }
        };
        SleeperInstanceArtefacts customArtefacts = new SleeperInstanceArtefacts(instanceProperties,
                new SleeperJarsFromProperties(instanceProperties,
                        new SleeperJarVersionIdProvider(jar -> jar.getArtifactId() + "-test-version")),
                images);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, customArtefacts, bucket, core);

        // Then
        assertThat(printer.toJson(stack))
                .contains(customImage)
                .doesNotContain(DockerDeployment.EKS_BULK_IMPORT.getDockerImageName(instanceProperties));
        assertThat(instanceProperties.get(BULK_IMPORT_EKS_IMAGE)).isEqualTo(customImage);
        assertThat(SparkConfigurationUtils.getSparkConfigurationForEKSFromInstanceProperties(instanceProperties))
                .containsEntry("spark.kubernetes.container.image", customImage);
    }

}
