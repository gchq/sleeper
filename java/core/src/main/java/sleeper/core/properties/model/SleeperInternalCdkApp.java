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
import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A model of known CDK apps that are part of Sleeper.
 */
public enum SleeperInternalCdkApp {
    STANDARD("sleeper.cdk.SleeperCdkApp", GetJarPath.cdkJarFile(), true),
    ARTEFACTS("sleeper.cdk.SleeperArtefactsCdkApp", GetJarPath.cdkJarFile(), false),
    DEMONSTRATION("sleeper.systemtest.cdk.SleeperDemonstrationCdkApp", GetJarPath.systemTestJarFile(), true),
    SYSTEM_TEST_INFRA("sleeper.systemtest.cdk.SystemTestInfrastructureCdkApp", GetJarPath.systemTestJarFile(), false);

    private final String cdkAppClassName;
    private final GetJarPath getCdkJarFile;
    private final boolean deploysSleeperInstance;

    SleeperInternalCdkApp(String cdkAppClassName, GetJarPath getCdkJarFile, boolean deploysSleeperInstance) {
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
        SleeperInternalCdkApp app = EnumUtils.getEnumIgnoreCase(SleeperInternalCdkApp.class, propertyValue);
        if (app == null) {
            return false;
        }
        return app.isDeploysSleeperInstance();
    }

    /**
     * Reads a string value that we expect to identify a CDK app used to deploy a Sleeper instance.
     *
     * @param  propertyValue the value
     * @return               the CDK app, if one matches
     */
    public static Optional<SleeperInternalCdkApp> readCdkAppDeployingSleeperInstance(String propertyValue) {
        SleeperInternalCdkApp app = EnumUtils.getEnumIgnoreCase(SleeperInternalCdkApp.class, propertyValue);
        if (app == null) {
            return Optional.empty();
        }
        return Optional.of(app).filter(SleeperInternalCdkApp::isDeploysSleeperInstance);
    }

    /**
     * Infers which app was used based on whether any system test properties are set.
     *
     * @param  instanceProperties the instance properties
     * @return                    the CDK app
     */
    public static SleeperInternalCdkApp inferBySystemTestProperties(InstanceProperties instanceProperties) {
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return SleeperInternalCdkApp.DEMONSTRATION;
        } else {
            return SleeperInternalCdkApp.STANDARD;
        }
    }

    /**
     * Generates a list of valid values for apps deploying a Sleeper instance, to be used in a Sleeper property
     * description. Uses each value as it appears in the enum.
     *
     * @return the valid values description
     */
    public static String describeCdkAppsDeployingSleeperInstance() {
        return Stream.of(values())
                .filter(SleeperInternalCdkApp::isDeploysSleeperInstance)
                .map(SleeperInternalCdkApp::toString)
                .map(string -> string.toLowerCase(Locale.ROOT))
                .collect(joining(", "));
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
            return (jarsDirectory, version) -> jarsDirectory.resolve(ClientJar.SYSTEM_TEST_CDK.getFormattedFilename(version));
        }
    }

}
