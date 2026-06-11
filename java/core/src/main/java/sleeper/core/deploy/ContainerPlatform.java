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
package sleeper.core.deploy;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A platform that a container image is built for. Combines an operating system and an architecture, in the same form
 * used by Docker (e.g. {@code linux/amd64}).
 *
 * @param os           the operating system
 * @param architecture the CPU architecture
 */
public record ContainerPlatform(String os, String architecture) {

    public static final ContainerPlatform LINUX_AMD64 = new ContainerPlatform("linux", "amd64");
    public static final ContainerPlatform LINUX_ARM64 = new ContainerPlatform("linux", "arm64");

    public ContainerPlatform {
        Objects.requireNonNull(os, "os must not be null");
        Objects.requireNonNull(architecture, "architecture must not be null");
    }

    /**
     * Parses a platform from its "os/architecture" string form.
     *
     * @param  value the string form, e.g. {@code linux/amd64}
     * @return       the platform
     */
    public static ContainerPlatform parse(String value) {
        int slash = value.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("Platform must be in the form os/architecture, got: " + value);
        }
        return new ContainerPlatform(value.substring(0, slash), value.substring(slash + 1));
    }

    /**
     * Builds an argument suitable to be passed to docker build as the value for the --platform option.
     *
     * @param  platforms the platforms
     * @return           the platform list argument
     */
    public static String buildPlatformListArgument(List<ContainerPlatform> platforms) {
        return platforms.stream()
                .map(ContainerPlatform::toString)
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return os + "/" + architecture;
    }
}
