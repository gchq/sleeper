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
package sleeper.core.properties.model;

import org.apache.commons.lang3.EnumUtils;

import sleeper.core.deploy.ClientJar;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * A model of known CDK apps that are part of Sleeper.
 */
public enum SleeperCdkApp {
    STANDARD("sleeper.cdk.SleeperCdkApp", GetJarPath.cdkJarFile(), true),
    ARTEFACTS("sleeper.cdk.SleeperArtefactsCdkApp", GetJarPath.cdkJarFile(), false),
    DEMONSTRATION("sleeper.systemtest.cdk.SleeperDemonstrationCdkApp", GetJarPath.systemTestJarFile(), true),
    SYSTEM_TEST_INFRA("sleeper.systemtest.cdk.SystemTestInfrastructureCdkApp", GetJarPath.systemTestJarFile(), false);

    private final String cdkAppClassName;
    private final GetJarPath getCdkJarFile;
    private final boolean deploysSleeperInstance;

    SleeperCdkApp(String cdkAppClassName, GetJarPath getCdkJarFile, boolean deploysSleeperInstance) {
        this.cdkAppClassName = cdkAppClassName;
        this.getCdkJarFile = getCdkJarFile;
        this.deploysSleeperInstance = deploysSleeperInstance;
    }

    /**
     * Validates whether a Sleeper property value identifies a CDK app that deploys a Sleeper instance.
     *
     * @param  propertyValue the property value
     * @return               true if it is a CDK app that deploys a Sleeper instance
     */
    public static boolean isCdkAppDeployingSleeperInstance(String propertyValue) {
        SleeperCdkApp app = EnumUtils.getEnumIgnoreCase(SleeperCdkApp.class, propertyValue);
        if (app == null) {
            return false;
        }
        return app.isDeploysSleeperInstance();
    }

    /**
     * Generates a list of valid values for apps deploying a Sleeper instance, to be used in a Sleeper property
     * description. Uses each value as it appears in the enum.
     *
     * @return the valid values description
     */
    public static String describeCdkAppsDeployingSleeperInstance() {
        return Stream.of(values()).filter(SleeperCdkApp::isDeploysSleeperInstance).toList().toString();
    }

    public String getCdkAppClassName() {
        return cdkAppClassName;
    }

    /**
     * Resolves a path to the jar file used to deploy a CDK app.
     *
     * @param  jarsDirectory the jars directory
     * @param  version       the Sleeper version
     * @return               the path to the jar file
     */
    public Path getCdkJarFile(Path jarsDirectory, String version) {
        return getCdkJarFile.getPathToCdkJarFile(jarsDirectory, version);
    }

    public boolean isDeploysSleeperInstance() {
        return deploysSleeperInstance;
    }

    /**
     * Resolves a path to the jar file used to deploy a CDK app.
     */
    private interface GetJarPath {

        /**
         * Resolves a path to the jar file used to deploy a CDK app.
         *
         * @param  jarsDirectory the jars directory
         * @param  version       the Sleeper version
         * @return               the path to the jar file
         */
        Path getPathToCdkJarFile(Path jarsDirectory, String version);

        /**
         * Resolves a path to the standard CDK jar file.
         *
         * @return the retriever to resolve the path
         */
        static GetJarPath cdkJarFile() {
            return (jarsDirectory, version) -> jarsDirectory.resolve(ClientJar.CDK.getFormattedFilename(version));
        }

        /**
         * Resolves a path to the system test CDK jar file.
         *
         * @return the retriever to resolve the path
         */
        static GetJarPath systemTestJarFile() {
            return (jarsDirectory, version) -> jarsDirectory.resolve(String.format("system-test-cdk-%s.jar", version));
        }
    }

}
