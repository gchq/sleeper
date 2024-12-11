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

import sleeper.build.notices.DependencyVersions.Dependency;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckNotices {

    private CheckNotices() {
    }

    public static void main(String[] args) throws IOException {
        Path mavenBase;
        Path noticesFile;
        if (args.length != 2) {
            mavenBase = findJavaDir();
            noticesFile = mavenBase.getParent().resolve("NOTICES");
        } else {
            noticesFile = Paths.get(args[0]);
            mavenBase = Paths.get(args[1]);
        }
        String notices = Files.readString(noticesFile);
        DependencyVersions versions = DependencyVersions.fromProjectBase(mavenBase);
        List<String> messages = findMissingNotices(notices, versions);
        if (!messages.isEmpty()) {
            System.err.println("Found missing notice declarations:");
            messages.forEach(System.err::println);
            System.exit(1);
        }
    }

    public static List<String> findMissingNotices(String notices, DependencyVersions versions) {
        return versions.getDependencies().stream()
                .flatMap(dependency -> findMissingNotice(notices, dependency).stream())
                .toList();
    }

    private static Optional<String> findMissingNotice(String notices, Dependency dependency) {
        Pattern pattern = Pattern.compile(dependency.groupId() + ":([^:]+):([^:]+)\\.\\*");
        Matcher matcher = pattern.matcher(notices);
        boolean foundDependency = false;
        while (matcher.find()) {
            String artifactIdFound = matcher.group(1);
            Pattern artifactIdPattern = Pattern.compile(artifactIdFound.replace("*", ".+"));
            if (artifactIdPattern.matcher(dependency.artifactId()).matches()) {
                return Optional.empty();
            }
            foundDependency = true;
        }
        if (foundDependency) {
            return Optional.of("Dependency artifact ID not matched: " + dependency.describe());
        } else {
            return Optional.of("Dependency not found: " + dependency.describe());
        }
    }

    private static Path findJavaDir() {
        return findJavaDir(Path.of(".").toAbsolutePath());
    }

    private static Path findJavaDir(Path currentPath) {
        for (int i = 0; i < currentPath.getNameCount(); i++) {
            Path part = currentPath.getName(i);
            if ("java".equals(String.valueOf(part))) {
                return currentPath.subpath(0, i);
            }
        }
        return currentPath.resolve("java");
    }
}
