/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.deploy;

import sleeper.core.SleeperVersion;
import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy client functions.
 */
public class ClientJar {

    private static final List<ClientJar> ALL = new ArrayList<>();

    public static final ClientJar BULK_IMPORT_RUNNER = withFormatAndImage("bulk-import-runner-%s.jar", "bulk-import-runner");
    public static final ClientJar CDK = withFormatAndImage("cdk-.jar", "cdk");
    public static final ClientJar CLIENTS_UTILITY = withFormatAndImage("clients-%s-utility.jar", "clients-utility");

    private final String filename;
    private final String imageName;
    private final boolean alwaysDockerDeploy;

    private ClientJar(String filename, String version, String imageName, boolean alwaysDockerDeploy) {
        this.filename = Objects.requireNonNull(filename, "filename must not be null");
        this.imageName = Objects.requireNonNull(imageName, "imageName must not be null");
        this.alwaysDockerDeploy = Objects.requireNonNull(alwaysDockerDeploy, "alwaysDockerDeploy must not be null");
    }

    /**
     * Creates a jar definition with a filename computed by adding the Sleeper version to the given format string.
     *
     * @param  format    the format string
     * @param  imageName the name of the Docker image built from this jar
     * @return           the jar definition
     */
    public static ClientJar withFormatAndImage(String format, String imageName) {
        ClientJar jar = new ClientJar(format, SleeperVersion.getVersion(), imageName, true);
        ALL.add(jar);
        return jar;
    }

    public String getFilename() {
        return filename;
    }

    public String getImageName() {
        return imageName;
    }

    public boolean isAlwaysDockerDeploy() {
        return alwaysDockerDeploy;
    }

    /**
     * Retrieves the name of the ECR repository for deploying this jar as a Docker container.
     *
     * @param  instanceProperties the instance properties
     * @return                    the ECR repository name
     */
    public String getEcrRepositoryName(InstanceProperties instanceProperties) {
        return DockerDeployment.getEcrRepositoryPrefix(instanceProperties) + "/" + imageName;
    }

    /**
     * Checks whether a given file is one of a specified list of jars.
     *
     * @param  file the file
     * @param  jars the jars
     * @return      true if the file is one of the given jars
     */
    public static boolean isFileJar(Path file, ClientJar... jars) {
        return isFilenameOfJar(String.valueOf(file.getFileName()), jars);
    }

    private static boolean isFilenameOfJar(String fileName, ClientJar... jars) {
        return Stream.of(jars)
                .map(ClientJar::getFilename)
                .anyMatch(fileName::equals);
    }

    @Override
    public String toString() {
        return "ClientJar{filename=" + filename + ", imageName=" + imageName + "}";
    }

    public static List<ClientJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }
}
