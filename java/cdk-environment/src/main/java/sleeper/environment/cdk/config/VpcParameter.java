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
package sleeper.environment.cdk.config;

import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.constructs.Construct;

public class VpcParameter {

    private final String vpcIdKey;

    private VpcParameter(String vpcIdKey) {
        this.vpcIdKey = vpcIdKey;
    }

    IVpc getOrDefault(AppContext context, Construct scope, IVpc defaultVpc) {
        return context.getStringOpt(vpcIdKey)
                .map(vpcId -> Vpc.fromLookup(scope, "Vpc", VpcLookupOptions.builder().vpcId(vpcId).build()))
                .orElse(defaultVpc);
    }

    static VpcParameter vpcIdKey(String vpcIdKey) {
        return new VpcParameter(vpcIdKey);
    }
}
