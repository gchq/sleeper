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
        check(notices, versions);
    }

    public static void check(String notices, DependencyVersions versions) {
        List<DependencyVersions.Dependency> failed = new ArrayList<>();
        for (DependencyVersions.Dependency dependency : versions.getDependencies()) {
            for (DependencyVersions.Version version : dependency.versions()) {
                Pattern pattern = Pattern.compile(dependency.groupId() + ":.+:" + version.major() + "\\.\\*");
                if (!pattern.matcher(notices).find()) {
                    failed.add(dependency);
                    break;
                }
            }
        }
        if (!failed.isEmpty()) {
            throw new MissingNoticeException(failed);
        }
    }

}
