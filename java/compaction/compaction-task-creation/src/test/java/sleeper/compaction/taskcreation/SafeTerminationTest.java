/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.taskcreation;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SafeTerminationTest {

    /**
     * Fake Lambda context class.
     */
    public static class FakeContext implements Context {

        @Override
        public String getAwsRequestId() {
            return null;
        }

        @Override
        public String getLogGroupName() {
            return null;
        }

        @Override
        public String getLogStreamName() {
            return null;
        }

        @Override
        public String getFunctionName() {
            return null;
        }

        @Override
        public String getFunctionVersion() {
            return null;
        }

        @Override
        public String getInvokedFunctionArn() {
            return null;
        }

        @Override
        public CognitoIdentity getIdentity() {
            return null;
        }

        @Override
        public ClientContext getClientContext() {
            return null;
        }

        @Override
        public int getRemainingTimeInMillis() {
            return 10000;
        }

        @Override
        public int getMemoryLimitInMB() {
            return 0;
        }

        @Override
        public LambdaLogger getLogger() {
            return null;
        }
    }

    /**
     * Custom termination Lambda input test from
     * <a href="https://docs.aws.amazon.com/autoscaling/ec2/userguide/lambda-custom-termination-policy.html">AWS documentation</a>.
     */
    public static final String IN_TEST = "{\n"
            + "  \"AutoScalingGroupARN\": \"arn:aws:autoscaling:us-east-1:<account-id>:autoScalingGroup:d4738357-2d40-4038-ae7e-b00ae0227003:autoScalingGroupName/my-asg\",\n"
            + "  \"AutoScalingGroupName\": \"my-asg\",\n"
            + "  \"CapacityToTerminate\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"Capacity\": 3,\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"Instances\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-0056faf8da3e1f75d\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-02e1c69383a3ed501\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-036bc44b6092c01c7\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"Cause\": \"SCALE_IN\"\n"
            + "}";

    public static final String IN_TEST_MULTI = "{\n"
            + "  \"AutoScalingGroupARN\": \"arn:aws:autoscaling:us-east-1:<account-id>:autoScalingGroup:d4738357-2d40-4038-ae7e-b00ae0227003:autoScalingGroupName/my-asg\",\n"
            + "  \"AutoScalingGroupName\": \"my-asg\",\n"
            + "  \"CapacityToTerminate\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"Capacity\": 3,\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"Capacity\": 4,\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"Instances\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-0056faf8da3e1f75d\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-02e1c69383a3ed501\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-036bc44b6092c01c7\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"InstanceId\": \"i-0056faf8da3e11234\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"InstanceId\": \"i-02e1c69383a34567\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"InstanceId\": \"i-036bc44b6092cbcdd\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"InstanceId\": \"i-036bc44b6092cabcd\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"Cause\": \"SCALE_IN\"\n"
            + "}";

    public static final String IN_TEST_ZERO = "{\n"
            + "  \"AutoScalingGroupARN\": \"arn:aws:autoscaling:us-east-1:<account-id>:autoScalingGroup:d4738357-2d40-4038-ae7e-b00ae0227003:autoScalingGroupName/my-asg\",\n"
            + "  \"AutoScalingGroupName\": \"my-asg\",\n"
            + "  \"CapacityToTerminate\": [\n"
            + "  ],\n"
            + "  \"Instances\": [\n"
            + "  ],\n"
            + "  \"Cause\": \"SCALE_IN\"\n"
            + "}";

    /**
     * Custom termination Lambda input test from
     * <a href="https://docs.aws.amazon.com/autoscaling/ec2/userguide/lambda-custom-termination-policy.html">AWS documentation</a>.
     */
    public static final String IN_TEST_LIMIT = "{\n"
            + "  \"AutoScalingGroupARN\": \"arn:aws:autoscaling:us-east-1:<account-id>:autoScalingGroup:d4738357-2d40-4038-ae7e-b00ae0227003:autoScalingGroupName/my-asg\",\n"
            + "  \"AutoScalingGroupName\": \"my-asg\",\n"
            + "  \"CapacityToTerminate\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1b\",\n"
            + "      \"Capacity\": 1,\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }"
            + "  ],\n"
            + "  \"Instances\": [\n"
            + "    {\n"
            + "      \"AvailabilityZone\": \"us-east-1c\",\n"
            + "      \"InstanceId\": \"i-0056faf8da3e1f75d\",\n"
            + "      \"InstanceType\": \"t2.nano\",\n"
            + "      \"InstanceMarketOption\": \"on-demand\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"Cause\": \"SCALE_IN\"\n"
            + "}";

    public static final String JSON_RESPONSE = "{\"InstanceIDs\":[\"id1\",\"id2\",\"id3\"]}";
    public static final String JSON_RESPONSE_LIMIT = "{\"InstanceIDs\":[\"id1\"]}";

    public static final List<InstanceDetails> INSTANCE_LIST = new ArrayList<>();
    public static final List<InstanceDetails> INSTANCE_LIST_LIMIT = new ArrayList<>();

    static {
        INSTANCE_LIST.add(new InstanceDetails("id1", "arn1", Instant.now(), 1, 1, 1, 1, 0, 0));
        INSTANCE_LIST.add(new InstanceDetails("id2", "arn2", Instant.now(), 1, 1, 1, 1, 0, 0));
        INSTANCE_LIST.add(new InstanceDetails("id3", "arn3", Instant.now(), 1, 1, 1, 1, 0, 0));

        INSTANCE_LIST_LIMIT.add(new InstanceDetails("id1", "arn1", Instant.now(), 1, 1, 1, 1, 0, 0));
    }

    @Test
    void throwOnNullDetails() {
        Context context = new FakeContext();
        assertThatThrownBy(() -> SafeTerminationLambda.findEmptyInstances(null, 0, context))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void throwOnNegativeSize() {
        List<InstanceDetails> instanceDetails = new ArrayList<>();
        Context context = new FakeContext();
        assertThatThrownBy(() -> SafeTerminationLambda.findEmptyInstances(instanceDetails, -1, context))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwOnNullContext() {
        List<InstanceDetails> instanceDetails = new ArrayList<>();
        assertThatThrownBy(() ->
                SafeTerminationLambda.findEmptyInstances(instanceDetails, 0, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldFindZeroEmptyInstances() {
        // Given
        List<InstanceDetails> details = new ArrayList<>();
        details.add(new InstanceDetails("id1", "someARN", Instant.now(), 1, 1, 1, 1, 1, 1));
        details.add(new InstanceDetails("id2", "someARN", Instant.now(), 1, 1, 1, 1, 0, 1));
        details.add(new InstanceDetails("id3", "someARN", Instant.now(), 1, 1, 1, 1, 1, 0));

        // When
        Set<String> empties = SafeTerminationLambda.findEmptyInstances(details, 3, new FakeContext());

        // Then
        assertThat(empties).isEmpty();
    }

    @Test
    void shouldFindOneEmptyInstances() {
        // Given
        List<InstanceDetails> details = new ArrayList<>();
        details.add(new InstanceDetails("id1", "someARN", Instant.now(), 1, 1, 1, 1, 1, 1));
        details.add(new InstanceDetails("id2", "someARN", Instant.now(), 1, 1, 1, 1, 0, 1));
        details.add(new InstanceDetails("id3", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));

        // When
        Set<String> single = SafeTerminationLambda.findEmptyInstances(details, 3, new FakeContext());

        // Then
        assertThat(single).containsExactly("id3");
    }

    @Test
    void shouldFindMultiEmptyInstances() {
        // Given
        List<InstanceDetails> details = new ArrayList<>();
        details.add(new InstanceDetails("id1", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));
        details.add(new InstanceDetails("id2", "someARN", Instant.now(), 1, 1, 1, 1, 0, 1));
        details.add(new InstanceDetails("id3", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));

        // When
        Set<String> multi = SafeTerminationLambda.findEmptyInstances(details, 3, new FakeContext());

        // Then
        assertThat(multi).containsExactlyInAnyOrder("id1", "id3");
    }

    @Test
    void shouldFindAndLimitMultiEmptyInstances() {
        // Given
        List<InstanceDetails> details = new ArrayList<>();
        details.add(new InstanceDetails("id1", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));
        details.add(new InstanceDetails("id2", "someARN", Instant.now(), 1, 1, 1, 1, 0, 1));
        details.add(new InstanceDetails("id3", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));

        // When
        Set<String> multi = SafeTerminationLambda.findEmptyInstances(details, 1, new FakeContext());

        // Then
        assertThat(multi).hasSize(1)
                .containsAnyOf("id1", "id3");
    }

    @Test
    void throwOnNullReader() {
        Writer output = new StringWriter();
        Context context = new FakeContext();
        assertThatThrownBy(() -> SafeTerminationLambda.suggestIDsToTerminate(null, output, INSTANCE_LIST, context))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void throwOnNullWriter() {
        Reader input = new StringReader("");
        Context context = new FakeContext();
        assertThatThrownBy(() -> SafeTerminationLambda.suggestIDsToTerminate(input, null, INSTANCE_LIST, context))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void throwOnNullInstances() {
        Reader input = new StringReader("");
        Writer output = new StringWriter();
        Context context = new FakeContext();
        assertThatThrownBy(() -> SafeTerminationLambda.suggestIDsToTerminate(input, output, null, context))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void throwOnNullContextSuggestions() {
        Reader input = new StringReader("");
        Writer output = new StringWriter();
        assertThatThrownBy(() -> SafeTerminationLambda.suggestIDsToTerminate(input, output, INSTANCE_LIST, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldWriteCorrectJSONOutputNoLimit() throws IOException {
        // Given
        StringReader input = new StringReader(IN_TEST);
        StringWriter output = new StringWriter();

        // When
        SafeTerminationLambda.suggestIDsToTerminate(input, output, INSTANCE_LIST, new FakeContext());

        // Then
        assertThat(output.toString()).isEqualToIgnoringWhitespace(JSON_RESPONSE);
    }

    @Test
    void shouldWriteCorrectJSONOutputWithLimit() throws IOException {
        // Given
        StringReader input = new StringReader(IN_TEST_LIMIT);
        StringWriter output = new StringWriter();

        // When
        SafeTerminationLambda.suggestIDsToTerminate(input, output, INSTANCE_LIST_LIMIT, new FakeContext());

        // Then
        assertThat(output.toString()).isEqualToIgnoringWhitespace(JSON_RESPONSE_LIMIT);
    }

    @Test
    void throwOnNullReaderTotalTerminations() {
        assertThatThrownBy(() -> SafeTerminationLambda.totalTerminations(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReturnZeroTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST_ZERO);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isZero();
    }

    @Test
    void shouldReturnSingleTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isEqualTo(3);
    }

    @Test
    void shouldReturnMultipleTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST_MULTI);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isEqualTo(7);
    }
}
