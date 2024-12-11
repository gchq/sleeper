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
import sleeper.build.notices.DependencyVersions.Version;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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
        List<NoticeDeclarationPattern> declarations = findNoticeDeclarations(notices);
        return versions.getDependencies().stream()
                .flatMap(dependency -> getMessageIfMissing(dependency, declarations).stream())
                .toList();
    }

    private static List<NoticeDeclarationPattern> findNoticeDeclarations(String notices) {
        Pattern pattern = Pattern.compile("([^:(), ]+):([^:(), ]+):([^:(), ]+)");
        List<NoticeDeclarationPattern> matches = new ArrayList<>();
        Matcher matcher = pattern.matcher(notices);
        while (matcher.find()) {
            matches.add(NoticeDeclarationPattern.fromParts(matcher.group(1), matcher.group(2), matcher.group(3)));
        }
        return matches;
    }

    private static Optional<String> getMessageIfMissing(Dependency dependency, List<NoticeDeclarationPattern> declarations) {
        boolean groupMatched = false;
        boolean artifactMatched = false;
        Set<String> versionsMatched = new TreeSet<>();
        for (NoticeDeclarationPattern declaration : declarations) {
            if (!declaration.groupId().matcher(dependency.groupId()).matches()) {
                continue;
            }
            groupMatched = true;
            if (!declaration.artifactId().matcher(dependency.artifactId()).matches()) {
                continue;
            }
            artifactMatched = true;
            dependency.versions().stream().map(Version::version)
                    .filter(version -> declaration.version().matcher(version).matches())
                    .forEach(versionsMatched::add);
        }
        if (!groupMatched) {
            return Optional.of("Dependency not found: " + dependency.describe());
        } else if (!artifactMatched) {
            return Optional.of("Dependency artifact ID not matched: " + dependency.describe());
        } else if (!versionsMatched.containsAll(dependency.versions().stream().map(Version::version).toList())) {
            return Optional.of("Dependency versions did not match: " + dependency.describe());
        } else {
            return Optional.empty();
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

    public record NoticeDeclarationPattern(Pattern groupId, Pattern artifactId, Pattern version) {
        public static NoticeDeclarationPattern fromParts(String groupId, String artifactId, String version) {
            return new NoticeDeclarationPattern(pattern(groupId), pattern(artifactId), pattern(version));
        }

        private static Pattern pattern(String string) {
            return Pattern.compile(string.replace("*", ".+"));
        }
    }
}
