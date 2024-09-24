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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import static sleeper.athena.metadata.SleeperMetadataHandler.SOURCE_TYPE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * An abstraction layer so that users can choose how to create a record iterator. Handles the writing of the records to
 * Athena and delegates the iterator creation to the implementation.
 */
public abstract class SleeperRecordHandler extends RecordHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperRecordHandler.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final InstanceProperties instanceProperties;

    public SleeperRecordHandler() {
        this(AmazonS3ClientBuilder.defaultClient(), AmazonDynamoDBClientBuilder.defaultClient(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SleeperRecordHandler(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, String configBucket) {
        super(SOURCE_TYPE);
        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDB);
    }

    public SleeperRecordHandler(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, String configBucket, AWSSecretsManager secretsManager, AmazonAthena athena) {
        super(s3Client, secretsManager, athena, SOURCE_TYPE);
        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDB);
    }

    /**
     * Reads and sends data to Athena for further processing. It allows the implementation to create the iterator which
     * will depend on the {@link com.amazonaws.athena.connector.lambda.handlers.MetadataHandler} supplying the splits.
     * The way that the iterator is created from the request will depend on implementation.
     *
     * @param  spiller            a mechanism to write data
     * @param  recordsRequest     the request from the user
     * @param  queryStatusChecker a means of checking the status of the query
     * @throws Exception          if something goes wrong
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws Exception {
        LOGGER.info("User {} with groups {} made data read request: {}", recordsRequest.getIdentity().getArn(), recordsRequest.getIdentity().getIamGroups(), recordsRequest);
        TableProperties tableProperties = tablePropertiesProvider.getByName(recordsRequest.getTableName().getTableName());

        Schema schema = createSchemaForDataRead(tableProperties.getSchema(), recordsRequest);
        CloseableIterator<Record> recordIterator = createRecordIterator(recordsRequest, schema, tableProperties);

        // Null indicates there is no data to read
        if (recordIterator == null) {
            return;
        }

        GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        configureBuilder(rowWriterBuilder, schema);

        GeneratedRowWriter writer = rowWriterBuilder.build();

        while (recordIterator.hasNext()) {
            Record next = recordIterator.next();
            spiller.writeRows((block, rowNum) -> writer.writeRow(block, rowNum, next) ? 1 : 0);
        }

        recordIterator.close();
    }

    /**
     * Implementation dependent code to create the schema used to read the data. Some implementations may be able to
     * slim down the schema to reduce the amount of data read per query, thereby making queries cheaper.
     *
     * @param  schema         the original schema associated with the table being queried
     * @param  recordsRequest the records request made by the user
     * @return                a schema to use for reading the files.
     */
    protected abstract Schema createSchemaForDataRead(Schema schema, ReadRecordsRequest recordsRequest);

    /**
     * Implementation dependent iterator creation code. The entire request which contains the user, split and schema is
     * passed to this method along with the table properties.
     *
     * @param    recordsRequest  the request
     * @param    schema          the table schema to use for reading
     * @param    tableProperties the table properties to use for reading the table
     * @return                   an iterator of records
     * @throws   Exception       when an iterator is not created
     * @implNote                 do not use the schema in the table properties as it could differ from the schema
     *                           provided
     */
    protected abstract CloseableIterator<Record> createRecordIterator(ReadRecordsRequest recordsRequest, Schema schema, TableProperties tableProperties) throws Exception;

    /**
     * Configures the writer so that it can write records from Sleeper to Athena.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param schema           the Sleeper schema for this table
     */
    private void configureBuilder(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Schema schema) {
        // Add Extractors according to the schema
        schema.getAllFields().forEach(field -> {
            Type type = field.getType();
            String name = field.getName();
            if (type instanceof IntType) {
                addIntExtractor(rowWriterBuilder, name);
            } else if (type instanceof LongType) {
                addLongExtractor(rowWriterBuilder, name);
            } else if (type instanceof StringType) {
                addStringExtractor(rowWriterBuilder, name);
            } else if (type instanceof ByteArrayType) {
                addByteArrayExtractor(rowWriterBuilder, name);
            } else if (type instanceof ListType) {
                addListExtractorFactory(rowWriterBuilder, name, (ListType) type);
            } else if (type instanceof MapType) {
                // do nothing as Maps aren't supported
            } else {
                throw new RuntimeException("Unrecognised type: " + type);
            }
        });

    }

    /**
     * Adds an extractor for byte arrays.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param name             the name of the field
     */
    private void addByteArrayExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, String name) {
        rowWriterBuilder.withExtractor(name, (VarBinaryExtractor) (context, dst) -> {
            Record record = (Record) context;
            dst.isSet = 1;
            dst.value = (byte[]) record.get(name);
        });
    }

    /**
     * Adds an extractor factory for Lists.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param name             the name of the field
     */
    private void addListExtractorFactory(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, String name, ListType type) {
        rowWriterBuilder.withFieldWriterFactory(name, (vector, extractor, constraint) -> (context, rowNum) -> {
            Record record = (Record) context;
            Object object = record.get(name);
            if (object != null) {
                BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, object);
            }
            return true;
        });
    }

    /**
     * Adds an extractor for Strings.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param name             the name of the field
     */
    private void addStringExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, String name) {
        rowWriterBuilder.withExtractor(name, (VarCharExtractor) (context, dst) -> {
            Record record = (Record) context;
            dst.isSet = 1;
            dst.value = (String) record.get(name);
        });
    }

    /**
     * Adds an extractor for Longs.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param name             the name of the field
     */
    private void addLongExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, String name) {
        rowWriterBuilder.withExtractor(name, (BigIntExtractor) (context, dst) -> {
            Record record = (Record) context;
            dst.isSet = 1;
            dst.value = (Long) record.get(name);
        });
    }

    /**
     * Adds an extractor for Integers.
     *
     * @param rowWriterBuilder the WriterBuilder
     * @param name             the name of the field
     */
    private void addIntExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, String name) {
        rowWriterBuilder.withExtractor(name, (IntExtractor) (context, dst) -> {
            Record record = (Record) context;
            dst.isSet = 1;
            dst.value = (Integer) record.get(name);
        });
    }

    /**
     * Gets the Hadoop configuration set in the table and instance.
     *
     * @param  tableProperties the table properties
     * @return                 the Hadoop configuration
     */
    protected Configuration getConfigurationForTable(TableProperties tableProperties) {
        Configuration conf = HadoopConfigurationProvider.getConfigurationForQueryLambdas(instanceProperties, tableProperties);
        return conf;
    }

    protected InstanceProperties getInstanceProperties() {
        return this.instanceProperties;
    }
}
