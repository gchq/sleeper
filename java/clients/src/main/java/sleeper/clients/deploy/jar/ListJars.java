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
package sleeper.clients.deploy.jar;

import sleeper.core.SleeperVersion;
import sleeper.core.deploy.ClientJar;
import sleeper.core.deploy.LambdaJar;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists fat jars used by scripts and deployment. For each jar, outputs the Maven artifact ID it is generated from,
 * its Maven classifier, and the filename it has in the scripts/jars directory. These are separated by colons, like
 * clients:utility:clients-1.2.3-utility.jar. Prints each jar on its own line.
 */
public class ListJars {

    private ListJars() {
    }

    public static void main(String[] args) {
        print(LambdaJar.getAll(), ClientJar.getAll(), SleeperVersion.getVersion(), System.out);
    }

    /**
     * Prints the given jars on separate lines. Uses the format artifactId:classifier:filename.
     *
     * @param lambdaJars all lambda jars
     * @param clientJars all client jars
     * @param version    the version of Sleeper
     * @param out        the stream to print to
     */
    public static void print(List<LambdaJar> lambdaJars, List<ClientJar> clientJars, String version, PrintStream out) {
        for (LambdaJar jar : lambdaJars) {
            out.println(jar.getArtifactId() + ":utility:" + jar.getFormattedFilename(version));
        }
        for (ClientJar jar : clientJars) {
            out.println(jar.getArtifactId() + ":utility:" + jar.getFormattedFilename(version));
        }
    }

}
