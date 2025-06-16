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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.athena.TestUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class RecordHandlerITBase extends LocalStackTestBase {

    // For storing data
    @TempDir
    public static Path tempDir;

    protected static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(
                    new Field("year", new IntType()),
                    new Field("month", new IntType()),
                    new Field("day", new IntType()))
            .sortKeyFields(
                    new Field("timestamp", new LongType()))
            .valueFields(
                    new Field("count", new LongType()),
                    new Field("map", new MapType(new StringType(), new StringType())),
                    new Field("str", new StringType()),
                    new Field("list", new ListType(new StringType())))
            .build();
    protected static final String SPILL_BUCKET_NAME = "spillbucket";
    protected static final String MIN_VALUE = Integer.toString(Integer.MIN_VALUE);
    protected StateStoreFactory stateStoreFactory;
    private InstanceProperties instanceProperties;

    @BeforeAll
    public static void createSpillBucket() {
        createBucket(SPILL_BUCKET_NAME);
    }

    @BeforeEach
    public void createInstance() throws IOException {
        this.instanceProperties = TestUtils.createInstance(s3Client, dynamoClient,
                createTempDirectory(tempDir, null).toString());
        this.stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    protected void assertFieldContainedValue(Block records, int position, String fieldName, Object expectedValue) {
        FieldReader fieldReader = records.getFieldReader(fieldName);
        fieldReader.setPosition(position);

        Object value = fieldReader.readObject();
        assertThat(value).isEqualTo(expectedValue);
    }

    protected TableProperties createTable(InstanceProperties instanceProperties, Object... initialSplits) throws IOException {
        TableProperties table = createEmptyTable(instanceProperties, initialSplits);
        TestUtils.ingestData(s3Client, dynamoClient, createTempDirectory(tempDir, null).toString(),
                instanceProperties, table);
        return table;
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties, Object... initialSplits) {
        return TestUtils.createTable(instanceProperties, SCHEMA, s3Client, dynamoClient, initialSplits);
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties, Schema schema, Object... initialSplits) {
        return TestUtils.createTable(instanceProperties, schema, s3Client, dynamoClient, initialSplits);
    }

    protected static org.apache.arrow.vector.types.pojo.Schema createArrowSchema() {
        return new SchemaBuilder()
                .addIntField("year")
                .addIntField("month")
                .addIntField("day")
                .addBigIntField("timestamp")
                .addBigIntField("count")
                .addStringField("str")
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();
    }
}
