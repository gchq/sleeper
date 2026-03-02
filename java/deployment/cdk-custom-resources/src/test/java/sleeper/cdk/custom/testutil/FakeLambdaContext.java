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
package sleeper.cdk.custom.testutil;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class FakeLambdaContext implements Context {

    @Override
    public String getAwsRequestId() {
        return "test-request";
    }

    @Override
    public String getLogGroupName() {
        return "test-log-group";
    }

    @Override
    public String getLogStreamName() {
        return "test-log-stream";
    }

    @Override
    public String getFunctionName() {
        return "test-function";
    }

    @Override
    public String getFunctionVersion() {
        return "test-function-version";
    }

    @Override
    public String getInvokedFunctionArn() {
        return "test-function-arn";
    }

    @Override
    public CognitoIdentity getIdentity() {
        return new FakeCognitoIdentity();
    }

    @Override
    public ClientContext getClientContext() {
        return new FakeClientContext();
    }

    @Override
    public int getRemainingTimeInMillis() {
        return 10000;
    }

    @Override
    public int getMemoryLimitInMB() {
        return 10000;
    }

    @Override
    public LambdaLogger getLogger() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLogger'");
    }

}
