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
package sleeper.clients.deploy.container;

import org.apache.commons.lang3.EnumUtils;

import java.util.Locale;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public enum DockerImageLocation {
    LOCAL_BUILD,
    REPOSITORY;

    public static DockerImageLocation parseOrNull(String string) {
        return EnumUtils.getEnumIgnoreCase(DockerImageLocation.class, string);
    }

    public static String describeOptions() {
        return Stream.of(values())
                .map(DockerImageLocation::toString)
                .map(string -> string.toLowerCase(Locale.ROOT))
                .collect(joining(", "));
    }
}
