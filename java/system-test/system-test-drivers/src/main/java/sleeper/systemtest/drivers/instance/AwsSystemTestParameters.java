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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import sleeper.systemtest.dsl.instance.SystemTestParameters;

public class AwsSystemTestParameters {

    private AwsSystemTestParameters() {
    }

    public static SystemTestParameters loadFromSystemProperties() {
        return SystemTestParameters.builder()
                .account(AWSSecurityTokenServiceClientBuilder.defaultClient()
                        .getCallerIdentity(new GetCallerIdentityRequest()).getAccount())
                .region(new DefaultAwsRegionProviderChain().getRegion().id())
                .loadFromSystemProperties()
                .build();
    }
}
