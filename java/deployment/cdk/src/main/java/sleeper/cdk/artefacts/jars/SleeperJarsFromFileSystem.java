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
package sleeper.cdk.artefacts.jars;

import software.amazon.awscdk.services.lambda.Code;
import software.constructs.Construct;

import sleeper.core.SleeperVersion;

import java.nio.file.Path;

public class SleeperJarsFromFileSystem implements SleeperJars {

    private final Path jarsDirectory;
    private final String version;

    public SleeperJarsFromFileSystem(Path jarsDirectory, String version) {
        this.jarsDirectory = jarsDirectory;
        this.version = version;
    }

    public static SleeperJarsFromFileSystem fromJarsDirectory(Path jarsDirectory) {
        return new SleeperJarsFromFileSystem(jarsDirectory, SleeperVersion.getVersion());
    }

    @Override
    public SleeperLambdaJars lambdaJarsAtScope(Construct scope) {
        return jar -> {
            String filename = jar.getFilename(version);
            String path = jarsDirectory.resolve(filename).toString();
            return Code.fromAsset(path);
        };
    }

}
