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
package sleeper.compaction.task.creation;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.EC2InstanceDetails;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class SafeTerminationLambda implements RequestStreamHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeTerminationLambda.class);
    /**
     * gson JSON encoder/decoder.
     */
    private static final Gson GSON = new Gson();
    /**
     * The amount of safe time for an API call before terminating.
     */
    private static final int SAFE_TIME_LIMIT = 200;

    private final EcsClient ecsClient;
    private final String ecsClusterName;

    public SafeTerminationLambda() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        this.ecsClient = EcsClient.create();

        // Find the instance properties from S3
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

        // Find ECS cluster name
        this.ecsClusterName = instanceProperties.get(COMPACTION_CLUSTER);
    }

    private String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || "".equals(parameter)) {
            throw new IllegalArgumentException("Missing environment variable: " + parameter);
        }
        return parameter;
    }

    /**
     * Examines the list of capacities that AWS AutoScaling has suggested for termination and sums
     * them.
     *
     * @param  reader              the input source
     * @return                     total capacity to be terminated
     * @throws JsonIOException     for a JSON related I/O error
     * @throws JsonSyntaxException if JSON is invalid
     */
    public static int totalTerminations(Reader reader) throws JsonIOException, JsonSyntaxException {
        Objects.requireNonNull(reader);
        JsonReader jsread = new JsonReader(reader);

        JsonElement root = JsonParser.parseReader(jsread);
        JsonArray capacities = root.getAsJsonObject().getAsJsonArray("CapacityToTerminate");

        int terminationCount = 0;

        // Loop over each element and extract the capacity count
        for (JsonElement e : capacities) {
            terminationCount += e.getAsJsonObject().getAsJsonPrimitive("Capacity").getAsInt();
        }
        return terminationCount;
    }

    /**
     * Examine list of suggested instances from AWS. Generate our own suggestions based on empty
     * instances.
     *
     * @param  input       input JSON
     * @param  output      response JSON
     * @param  detailsIt   iterator of cluster instance details
     * @param  context     the AWS Lambda context
     * @throws IOException if anything goes wrong
     */
    public static void suggestIDsToTerminate(Reader input, Writer output, Iterable<EC2InstanceDetails> detailsIt,
            Context context) throws IOException {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(detailsIt, "detailsIt");
        Objects.requireNonNull(context, "context");

        // Total the number of terminations to make
        int suggestTerminationCount = totalTerminations(input);

        LOGGER.info("AWS AutoScaling wants to terminate {} instances", suggestTerminationCount);

        // Filter out ones that are not running tasks
        Set<String> emptyInstances = findEmptyInstances(detailsIt, suggestTerminationCount, context);

        LOGGER.info("Returned list of instances to terminate {}", emptyInstances);

        // Return this back to AWS
        Map<String, Set<String>> returnData = new HashMap<>();
        returnData.put("InstanceIDs", emptyInstances);
        String outputJson = GSON.toJson(returnData);

        output.write(outputJson);
    }

    /**
     * Filter out a set of instances that are not running and RUNNING/PENDING tasks.
     *
     * @param  detailsIt                iterator of instances in cluster
     * @param  suggestedSize            limit for number of instances to return
     * @param  context                  AWS Lambda context
     * @return                          set of empty instances
     * @throws NullPointerException     for clusterDetails
     * @throws IllegalArgumentException if suggestedSize < 0
     */
    public static Set<String> findEmptyInstances(Iterable<EC2InstanceDetails> detailsIt, int suggestedSize,
            Context context) {
        Objects.requireNonNull(detailsIt, "detailsIt");
        Objects.requireNonNull(context, "context");
        if (suggestedSize < 0) {
            throw new IllegalArgumentException("suggested size < 0");
        }

        Set<String> emptyInstances = new TreeSet<>();

        for (EC2InstanceDetails d : detailsIt) {
            if (d.numPendingTasks + d.numRunningTasks == 0) {
                emptyInstances.add(d.instanceId);
            }
            // Running out of time?
            if (emptyInstances.size() >= suggestedSize || context.getRemainingTimeInMillis() < SAFE_TIME_LIMIT) {
                break;
            }
        }
        return emptyInstances;
    }

    /**
     * Process request from AWS Lambda. Sets up a {@link java.io.Reader} and a
     * {@link java.io.Writer} around the streams.
     *
     * @param input   the incoming Lambda event data
     * @param output  the response JSON
     * @param context event context
     */
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8))) {

            suggestIDsToTerminate(reader, out, () -> EC2InstanceDetails.streamInstances(ecsClusterName, ecsClient).iterator(), context);

        } catch (IllegalStateException | JsonSyntaxException e) {
            LOGGER.error("Error reading/writing JSON response", e);
        }
    }
}
