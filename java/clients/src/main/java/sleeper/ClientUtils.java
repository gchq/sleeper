/*
 * Copyright 2022 Crown Copyright
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
package sleeper;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.util.Optional;

/**
 *
 */
public class ClientUtils {

    private ClientUtils() {
    }

    public static InstanceProperties getInstanceProperties(String instanceId) throws IOException {
        return getInstanceProperties(AmazonS3ClientBuilder.defaultClient(), instanceId);
    }

    public static InstanceProperties getInstanceProperties(AmazonS3 amazonS3, String instanceId) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(amazonS3, instanceId);
        return instanceProperties;
    }

    public static Optional<String> optionalArgument(String[] args, int index) {
        if (args.length > index) {
            return Optional.of(args[index]);
        } else {
            return Optional.empty();
        }
    }
}
