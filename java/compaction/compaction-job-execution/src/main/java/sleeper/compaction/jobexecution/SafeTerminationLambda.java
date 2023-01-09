package sleeper.compaction.jobexecution;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
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
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;

public class SafeTerminationLambda implements RequestStreamHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeTerminationLambda.class);
    /** gson JSON encoder/decoer */
    private static final Gson GSON = new Gson();
    /** Type parameter for converting the inner map to a Java type. */
    private static final TypeToken<Map<String, String>> TYPE_TOKEN = new TypeToken<Map<String, String>>() {
    };
    /** JSON reader for decoding. */
    private static final JsonParser PARSER = new JsonParser();

    private final AmazonS3 s3Client;
    private final AmazonECS ecsClient;
    private final String ecsClusterName;

    public SafeTerminationLambda() throws IOException {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        String type = validateParameter("type");
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.ecsClient = AmazonECSClientBuilder.defaultClient();

        // find the instance properties from S3
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        // find ECS cluster name
        if (type.equals("compaction")) {
            this.ecsClusterName = instanceProperties.get(COMPACTION_CLUSTER);
        } else if (type.equals("splittingcompaction")) {
            this.ecsClusterName = instanceProperties.get(SPLITTING_COMPACTION_CLUSTER);
        } else {
            throw new RuntimeException("type should be 'compaction' or 'splittingcompaction'");
        }
    }

    private String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || "".equals(parameter)) {
            throw new IllegalArgumentException("Missing environment variable: " + parameter);
        }
        return parameter;
    }

    /**
     * Gets the list of EC2 instance IDs suggested for termination from the input JSON.
     * 
     * @param reader the input source
     * @return set of IDs
     * @throws NullPointerException if reader is null
     * @throws JsonIOException for an JSON related I/O error
     * @throws JsonSyntaxException if JSON is invalid
     */
    public static Set<String> extractSuggestedIDs(Reader reader) throws JsonIOException, JsonSyntaxException {
        Objects.requireNonNull(reader, "reader");
        JsonReader jsread = new JsonReader(reader);

        JsonElement root = PARSER.parse(jsread);
        JsonArray arr = root.getAsJsonObject().getAsJsonArray("Instances");

        // set of instance IDs that AWS Auto Scaling wants to terminate
        Set<String> suggestedTerminations = new HashSet<>();

        // loop over each element and extract the instance ID
        for (JsonElement e : arr) {
            Map<String, String> instance = GSON.fromJson(e, TYPE_TOKEN.getType());
            suggestedTerminations.add(instance.get("InstanceId"));
        }
        return suggestedTerminations;
    }

    /**
     * Examine list of suggested instances from AWS. Generate our own suggestions based on empty
     * instances.
     * 
     * @param input input JSON
     * @param output response JSON
     * @param clusterDetails details of machines in cluster
     * @throws IOException if anything goes wrong
     */
    public static void suggestIDsToTerminate(Reader input, Writer output, Map<String, InstanceDetails> clusterDetails) throws IOException {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(clusterDetails, "clusterDetails");

        // get the list of suggested termination IDs
        Set<String> suggestedTerminations = extractSuggestedIDs(input);

        LOGGER.info("Suggested instances for termination from AWS {}", suggestedTerminations);

        // filter out ones that are not running tasks
        Set<String> emptyInstances = findEmptyInstances(clusterDetails, suggestedTerminations.size());

        LOGGER.info("Returned list of instances to terminate {}", emptyInstances);

        // return this back to AWS
        Map<String, Set<String>> returnData = new HashMap<>();
        returnData.put("InstanceIDs", emptyInstances);
        String outputJson = GSON.toJson(returnData);

        output.write(outputJson);
    }

    /**
     * Filter out a set of instances that are not running and RUNNING/PENDING tasks.
     * 
     * @param clusterDetails all instances in cluster
     * @param suggestedSize limit for number of instances to return
     * @return set of empty instances
     * @throws NullPointerException for clusterDetails
     * @throws IllegalArgumentException if suggestedSize < 0
     */
    public static Set<String> findEmptyInstances(Map<String, InstanceDetails> clusterDetails, int suggestedSize) {
        Objects.requireNonNull(clusterDetails, "clusterDetails");
        if (suggestedSize < 0) {
            throw new IllegalArgumentException("suggested size < 0");
        }
        Set<String> emptyInstances = clusterDetails.entrySet().stream()
                        // find the instances that are not running any tasks
                        .filter(e -> (e.getValue().numPendingTasks + e.getValue().numRunningTasks) == 0)
                        // just get the instance ID
                        .map(Map.Entry::getKey)
                        // don't return more than was suggested initially
                        .limit(suggestedSize)
                        .collect(Collectors.toSet());
        return emptyInstances;
    }

    /**
     * Process request from AWS Lambda. Sets up a {@link java.io.Reader} and a
     * {@link java.io.Writer} around the streams.
     * 
     * @param input the incoming Lambda event data
     * @param output the response JSON
     * @param context event context
     */
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
                        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8))) {

            suggestIDsToTerminate(reader, out, InstanceDetails.fetchInstanceDetails(this.ecsClusterName, ecsClient));

        } catch (IllegalStateException | JsonSyntaxException e) {
            LOGGER.error("Error reading/writing JSON response", e);
        }
    }
}
