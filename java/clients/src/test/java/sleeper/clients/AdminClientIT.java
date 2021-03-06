package sleeper.clients;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;
import static sleeper.configuration.properties.table.TableProperty.ENCRYPTED;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AdminClientIT {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

    private static final Schema KEY_VALUE_SCHEMA = new Schema();
    static {
        KEY_VALUE_SCHEMA.setRowKeyFields(new Field("key", new StringType()));
        KEY_VALUE_SCHEMA.setValueFields(new Field("value", new StringType()));
    }

    private static final String INSTANCE_ID = "instance";
    private static final String CONFIG_BUCKET_NAME = "sleeper-" + INSTANCE_ID + "-config";
    private static final String TABLE_NAME_VALUE = "test";

    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    @Test
    public void shouldLoadAllInstancePropertiesFromS3AndPrintThemToSystemOut() throws IOException {
        // Given
        System.setOut(new PrintStream(outputStreamCaptor));
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        // When
        AdminClient.printInstancePropertiesReport(getS3Client(), INSTANCE_ID);

        // Then check some default property values are present in the output, don't check values in case they change
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.athena.handler.memory"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.bulk.import.emr.bucket.create"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.bulk.import.emr.bucket.create"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.default.page.size"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.query.tracker.ttl.days"));

        // Then check some set property values are present in the output
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.account: 1234567890"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.log.retention.days: 1"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.tags: name,abc,project,test"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.vpc: aVPC"));

        // Then check the ordering of some property names are correct
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.account")
                        < outputStreamCaptor.toString().indexOf("sleeper.log.retention.days"));
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.account")
                        < outputStreamCaptor.toString().indexOf("sleeper.vpc"));
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.log.retention.days")
                        < outputStreamCaptor.toString().indexOf("sleeper.vpc"));
    }

    @Test
    public void shouldLoadAllTablePropertiesFromS3AndPrintThemToSystemOut() throws IOException {
        // Given
        System.setOut(new PrintStream(outputStreamCaptor));
        getS3Client().createBucket(CONFIG_BUCKET_NAME);

        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        validInstanceProperties.set(TABLE_PROPERTIES, TABLES_PREFIX + "/" + TABLE_NAME_VALUE);
        validInstanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        validTableProperties.saveToS3(getS3Client());

        // When
        AdminClient.printTablePropertiesReport(getS3Client(), INSTANCE_ID, TABLE_NAME_VALUE);

        // Then check some default table property values are present in the output, don't check values in case they change
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.splits.base64.encoded"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.statestore.classname"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.fs.s3a.readahead.range"));

        // Then check some set table property values are present in the output
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.name: test"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.encrypted: false"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("sleeper.table.schema: " +
                "{\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]," +
                "\"sortKeyFields\":[]," +
                "\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}"));

        // Then check the ordering of some property names are correct
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.table.encrypted")
                        < outputStreamCaptor.toString().indexOf("sleeper.table.name"));
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.table.encrypted")
                        < outputStreamCaptor.toString().indexOf("sleeper.table.schema"));
        Assert.assertTrue(outputStreamCaptor.toString().indexOf("sleeper.table.name")
                        < outputStreamCaptor.toString().indexOf("sleeper.table.schema"));
    }

    @Test
    public void shouldListAllTablesAndPrintThemToSystemOut() throws IOException {
        // Given
        System.setOut(new PrintStream(outputStreamCaptor));
        getS3Client().createBucket(CONFIG_BUCKET_NAME);

        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        validInstanceProperties.set(TABLE_PROPERTIES, TABLES_PREFIX + "/" + TABLE_NAME_VALUE);
        validInstanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        validTableProperties.saveToS3(getS3Client());

        TableProperties validTableProperties2 = createValidTableProperties(validInstanceProperties, "test2");
        validTableProperties2.saveToS3(getS3Client());

        // When
        AdminClient.printTablesReport(getS3Client(), INSTANCE_ID);

        // Then check some table names are present in the output
        Assert.assertTrue(outputStreamCaptor.toString().contains("test"));
        Assert.assertTrue(outputStreamCaptor.toString().contains("test2"));
    }

    @Test
    public void shouldUpdateInstancePropertyInS3() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        // When
        AdminClient.updateProperty(getS3Client(), INSTANCE_ID, LOG_RETENTION_IN_DAYS.getPropertyName(), "3", null);

        // Then
        InstanceProperties updatedInstanceProperties = new InstanceProperties();
        updatedInstanceProperties.loadFromS3(getS3Client(), CONFIG_BUCKET_NAME);
        Assert.assertEquals("3", updatedInstanceProperties.get(LOG_RETENTION_IN_DAYS));
    }

    @Test
    public void shouldUpdateTablePropertyInS3() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());
        validTableProperties.saveToS3(getS3Client());

        // When
        AdminClient.updateProperty(getS3Client(), INSTANCE_ID, ENCRYPTED.getPropertyName(),
                "true", TABLE_NAME_VALUE);

        // Then
        TablePropertiesProvider tablePropertiesProvider =
                new TablePropertiesProvider(getS3Client(), validInstanceProperties);
        TableProperties updateTableProperties = tablePropertiesProvider.getTableProperties(TABLE_NAME_VALUE);
        Assert.assertEquals("true", updateTableProperties.get(ENCRYPTED));
    }

    @Test
    public void shouldThrowErrorWhenInstancePropertyIsInvalid() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                AdminClient.updateProperty(getS3Client(), INSTANCE_ID, LOG_RETENTION_IN_DAYS.getPropertyName(),"abc", null));
        // Then
        String expectedMessage = "Sleeper property: " + LOG_RETENTION_IN_DAYS.getPropertyName() + " is invalid";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    @Test
    public void shouldThrowErrorWhenTablePropertyIsInvalid() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());
        validTableProperties.saveToS3(getS3Client());

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                AdminClient.updateProperty(getS3Client(), INSTANCE_ID, ENCRYPTED.getPropertyName(),
                        "abc", TABLE_NAME_VALUE));
        // Then
        String expectedMessage = "Sleeper property: " + ENCRYPTED.getPropertyName() + " is invalid";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    @Test
    public void shouldThrowErrorWhenInstancePropertyDoesNotExist() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                AdminClient.updateProperty(getS3Client(), INSTANCE_ID,
                        "sleeper.log.ret.day", "3", null));
        // Then
        String expectedMessage = "Sleeper property: sleeper.log.ret.day does not exist and cannot be updated";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    @Test
    public void shouldThrowErrorWhenTablePropertyDoesNotExist() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());
        validTableProperties.saveToS3(getS3Client());

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                AdminClient.updateProperty(getS3Client(), INSTANCE_ID, "sleeper.table.encrypt",
                        "true", TABLE_NAME_VALUE));
        // Then
        String expectedMessage = "Sleeper property: sleeper.table.encrypt does not exist and cannot be updated";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    @Test
    public void shouldThrowErrorWhenTableIsNotProvided() throws IOException {
        // Given
        InstanceProperties validInstanceProperties = createValidInstanceProperties();
        TableProperties validTableProperties = createValidTableProperties(validInstanceProperties, TABLE_NAME_VALUE);
        getS3Client().createBucket(CONFIG_BUCKET_NAME);
        validInstanceProperties.saveToS3(getS3Client());
        validTableProperties.saveToS3(getS3Client());

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                AdminClient.updateProperty(getS3Client(), INSTANCE_ID, ENCRYPTED.getPropertyName(),
                        "true", null));
        // Then
        String expectedMessage = "When a table property is being updated e.g. sleeper.table.* " +
                "then a Table Name must be provided in the parameters";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    @After
    public void tearDown() {
        System.setOut(standardOut);
    }

    private InstanceProperties createValidInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, "test");
        instanceProperties.set(ACCOUNT, "1234567890");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(JARS_BUCKET, "bucket");
        instanceProperties.set(SUBNET, "subnet1");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        return instanceProperties;
    }

    private TableProperties createValidTableProperties(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(ENCRYPTED, "false");
        return tableProperties;
    }
}
