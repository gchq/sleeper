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
package sleeper.core;

/**
 * Fixes versions of images for TestContainers.
 */
public final class CommonTestConstants {
    // Temporarily using a different version of LocalStack for tests with SQS SDK v2, see issue:
    // https://github.com/gchq/sleeper/issues/3449
    public static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:1.4.0";
    public static final String LOCALSTACK_DOCKER_IMAGE_V2 = "localstack/localstack:3.8.1";

    private CommonTestConstants() {
        // Empty
    }
}
