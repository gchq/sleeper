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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class SafeTerminationTest {

    /** Fake Lambda context class. */
    public static class FakeContext implements Context {

        @Override
        public String getAwsRequestId() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getLogGroupName() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getLogStreamName() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getFunctionName() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getFunctionVersion() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getInvokedFunctionArn() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CognitoIdentity getIdentity() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ClientContext getClientContext() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getRemainingTimeInMillis() {
            return 10000;
        }

        @Override
        public int getMemoryLimitInMB() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public LambdaLogger getLogger() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    /**
     * Custom termination Lambda input test from
     * https://docs.aws.amazon.com/autoscaling/ec2/userguide/lambda-custom-termination-policy.html.
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
     * https://docs.aws.amazon.com/autoscaling/ec2/userguide/lambda-custom-termination-policy.html.
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

    public static final Set<String> EXPECTED_SET = new HashSet<>(Arrays.asList("i-0056faf8da3e1f75d", "i-02e1c69383a3ed501", "i-036bc44b6092c01c7"));

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

    @Test(expected = NullPointerException.class)
    public void throwOnNullDetails() {
        SafeTerminationLambda.findEmptyInstances(null, 0, new FakeContext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwOnNegativeSize() {
        SafeTerminationLambda.findEmptyInstances(new ArrayList<>(), -1, new FakeContext());
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullContext() {
        SafeTerminationLambda.findEmptyInstances(new ArrayList<>(), 0, null);
    }

    @Test
    public void shouldFindZeroEmptyInstances() {
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
    public void shouldFindOneEmptyInstances() {
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
    public void shouldFindMultiEmptyInstances() {
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
    public void shouldFindAndLimitMultiEmptyInstances() {
        // Given
        List<InstanceDetails> details = new ArrayList<>();
        details.add(new InstanceDetails("id1", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));
        details.add(new InstanceDetails("id2", "someARN", Instant.now(), 1, 1, 1, 1, 0, 1));
        details.add(new InstanceDetails("id3", "someARN", Instant.now(), 1, 1, 1, 1, 0, 0));

        // When
        Set<String> multi = SafeTerminationLambda.findEmptyInstances(details, 1, new FakeContext());

        // Then
        assertThat(multi).hasSize(1);
        assertThat(multi).containsAnyOf("id1", "id3");
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullReader() throws IOException {
        SafeTerminationLambda.suggestIDsToTerminate(null, new StringWriter(), INSTANCE_LIST, new FakeContext());
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullWriter() throws IOException {
        SafeTerminationLambda.suggestIDsToTerminate(new StringReader(""), null, INSTANCE_LIST, new FakeContext());
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullInstances() throws IOException {
        SafeTerminationLambda.suggestIDsToTerminate(new StringReader(""), new StringWriter(), null, new FakeContext());
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullContextSuggestions() throws IOException {
        SafeTerminationLambda.suggestIDsToTerminate(new StringReader(""), new StringWriter(), INSTANCE_LIST, null);
    }

    @Test
    public void shouldWriteCorrectJSONOutputNoLimit() throws IOException {
        // Given
        StringReader input = new StringReader(IN_TEST);
        StringWriter output = new StringWriter();

        // When
        SafeTerminationLambda.suggestIDsToTerminate(input, output, INSTANCE_LIST, new FakeContext());

        // Then
        assertThat(output.toString()).isEqualToIgnoringWhitespace(JSON_RESPONSE);
    }

    @Test
    public void shouldWriteCorrectJSONOutputWithLimit() throws IOException {
        // Given
        StringReader input = new StringReader(IN_TEST_LIMIT);
        StringWriter output = new StringWriter();

        // When
        SafeTerminationLambda.suggestIDsToTerminate(input, output, INSTANCE_LIST_LIMIT, new FakeContext());

        // Then
        assertThat(output.toString()).isEqualToIgnoringWhitespace(JSON_RESPONSE_LIMIT);
    }

    @Test(expected = NullPointerException.class)
    public void throwOnNullReaderTotalTerminations() {
        SafeTerminationLambda.totalTerminations(null);
    }

    @Test
    public void shouldReturnZeroTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST_ZERO);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isEqualTo(0);
    }

    @Test
    public void shouldReturnSingleTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isEqualTo(3);
    }

    @Test
    public void shouldReturnMultipleTotalTerminations() {
        // Given
        StringReader input = new StringReader(IN_TEST_MULTI);

        // When
        int totalCapacity = SafeTerminationLambda.totalTerminations(input);

        // Then
        assertThat(totalCapacity).isEqualTo(7);
    }
}
