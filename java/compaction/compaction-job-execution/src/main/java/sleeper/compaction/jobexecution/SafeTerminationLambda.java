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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;

public class SafeTerminationLambda implements RequestStreamHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeTerminationLambda.class);
    private static final Gson GSON = new Gson();
    private static final TypeToken<Map<String, String>> TYPE_TOKEN = new TypeToken<Map<String, String>>() {
    };
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
     * @throws JsonIOException for an JSON related I/O error
     * @throws JsonSyntaxException if JSON is invalid
     */
    private static Set<String> extractSuggestedIDs(Reader reader) throws JsonIOException, JsonSyntaxException {
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

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));

        // get the list of suggested termination IDs
        Set<String> suggestedTerminations = extractSuggestedIDs(reader);

        LOGGER.info("Suggested instances for termination {}", suggestedTerminations);

        // get cluster instance details
        Map<String, InstanceDetails> clusterDetails = InstanceDetails.fetchInstanceDetails(this.ecsClusterName, ecsClient);

        // filter out ones that are not running tasks
        
    }
}
