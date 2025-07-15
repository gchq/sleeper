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
package sleeper.clients.deploy.container;

import java.io.IOException;

/**
 * Callbacks to be invoked while building and uploading Docker images. This may be used to authenticate with Docker,
 * and/or to create the repositories being uploaded to, e.g. in AWS ECR.
 */
public interface UploadDockerImagesCallbacks {

    UploadDockerImagesCallbacks NONE = new UploadDockerImagesCallbacks() {
    };

    default void beforeAll() throws IOException, InterruptedException {
    }

    default void beforeEach(StackDockerImage image) {
    }

    default void onFail(StackDockerImage image, Exception e) {
    }
}
