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
package sleeper.cdk.custom;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.LambdaHandler;

import static org.assertj.core.api.Assertions.assertThat;

public class CdkCustomLambdaHandlerTest {

    @Test
    void shouldMatchAutoDeleteLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.AUTO_DELETE_S3_OBJECTS.getHandler())
                .isEqualTo(AutoDeleteS3ObjectsLambda.class.getName() + "::handleEvent");
    }

    @Test
    void shouldMatchPropertiesLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.INSTANCE_PROPERTIES_WRITER.getHandler())
                .isEqualTo(InstancePropertiesWriterLambda.class.getName() + "::handleEvent");
    }

    @Test
    void shouldMatchVpcCheckLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.VPC_CHECK.getHandler())
                .isEqualTo(VpcCheckLambda.class.getName() + "::handleEvent");
    }

}
