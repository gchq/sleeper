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
package sleeper.build.notices;

import sleeper.build.maven.DependencyVersions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckNotices {

    private CheckNotices() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected 2 arguments: <path-to-notices-file> <path-to-maven-project-base>");
        }
        Path noticesFile = Paths.get(args[0]);
        Path mavenBase = Paths.get(args[1]);
        String notices = Files.readString(noticesFile);
        DependencyVersions versions = DependencyVersions.fromProjectBase(mavenBase);
        List<DependencyVersions.Dependency> dependencies = findDependenciesMissingNotices(notices, versions);
        if (!dependencies.isEmpty()) {
            System.err.println("Found dependencies missing notice declarations:");
            for (DependencyVersions.Dependency dependency : dependencies) {
                System.err.println(dependency.describe());
            }
            System.exit(1);
        }
    }

    public static List<DependencyVersions.Dependency> findDependenciesMissingNotices(String notices, DependencyVersions versions) {
        List<DependencyVersions.Dependency> failed = new ArrayList<>();
        for (DependencyVersions.Dependency dependency : versions.getDependencies()) {
            for (DependencyVersions.Version version : dependency.versions()) {
                Pattern pattern = Pattern.compile(dependency.groupId() + ":(.+):" + version.major() + "\\.\\*");
                Matcher matcher = pattern.matcher(notices);
                if (!matcher.find()) {
                    failed.add(dependency);
                    break;
                }
                Pattern artifactIdPattern = Pattern.compile(matcher.group(1).replace("*", ".+"));
                if (!artifactIdPattern.matcher(dependency.artifactId()).matches()) {
                    failed.add(dependency);
                    break;
                }
            }
        }
        return failed;
    }

}
