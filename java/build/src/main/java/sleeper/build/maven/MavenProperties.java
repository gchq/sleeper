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
package sleeper.build.maven;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MavenProperties {

    private MavenProperties() {
    }

    private static final Pattern REFERENCE = Pattern.compile("\\$\\{([^}]+)\\}");

    public static String resolve(String string, Map<String, String> properties) {
        Matcher matcher = REFERENCE.matcher(string);
        StringBuffer resolved = new StringBuffer();
        int index = 0;
        while (matcher.find()) {
            resolved.append(string.substring(index, matcher.start()));
            String propertyName = matcher.group(1);
            resolved.append(resolve(properties.get(propertyName), properties));
            index = matcher.end();
        }
        resolved.append(string.substring(index, string.length()));
        return resolved.toString();
    }

}
