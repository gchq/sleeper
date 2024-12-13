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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record NoticeDeclaration(int number, String declaration, Pattern groupId, Pattern artifactId, Pattern version) {
    public static NoticeDeclaration from(int number, String declaration, String groupId, String artifactId, String version) {
        return new NoticeDeclaration(number, declaration, pattern(groupId), pattern(artifactId), pattern(version));
    }

    public static List<NoticeDeclaration> findDeclarations(String notices) {
        Pattern pattern = Pattern.compile("([^:(), ]+):([^:(), ]+):([^:(), ]+)");
        List<NoticeDeclaration> matches = new ArrayList<>();
        Matcher matcher = pattern.matcher(notices);
        int number = 0;
        while (matcher.find()) {
            matches.add(NoticeDeclaration.from(number, matcher.group(), matcher.group(1), matcher.group(2), matcher.group(3)));
            number++;
        }
        return matches;
    }

    private static Pattern pattern(String string) {
        return Pattern.compile(string.replace("*", ".+"));
    }

    public String unmatchedMessage() {
        return "Dependency not present in pom.xml: " + declaration;
    }
}
