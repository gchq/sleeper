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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class StateMachineExecutorTest {
    private AWSStepFunctions stepFunctions;
    private TablePropertiesProvider tablePropertiesProvider;
    private AtomicReference<StartExecutionRequest> requested;
    private AmazonS3 amazonS3;

    @Before
    public void setUpStepFunctions() {
        requested = new AtomicReference<>();
        stepFunctions = mock(AWSStepFunctions.class);
        amazonS3 = mock(AmazonS3Client.class);
        tablePropertiesProvider = mock(TablePropertiesProvider.class);
        when(stepFunctions.startExecution(any(StartExecutionRequest.class)))
                .then((Answer<StartExecutionResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return null;
                });
        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> new TableProperties(new InstanceProperties()));
    }

    @Test
    public void shouldPassJobToStepFunctions() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonObject jsonJobObject = parsed.getAsJsonObject().getAsJsonObject("job");
        BulkImportJob bulkImportJob = new Gson().fromJson(jsonJobObject, BulkImportJob.class);
        assertThat(bulkImportJob).isEqualTo(myJob);
    }

    @Test
    public void shouldPassJobIdToSparkConfig() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .filter(JsonElement::isJsonPrimitive) // Filters out the null reference caused by the null config bucket
                .map(JsonElement::getAsString))
                .containsOnlyOnce("spark.app.name=my-job");
    }

    @Test
    public void shouldUseDefaultConfigurationIfNoneSpecified() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .filter(JsonElement::isJsonPrimitive) // Filters out the null reference caused by the null config bucket
                .map(JsonElement::getAsString))
                .contains("--conf");
    }

    @Test
    public void shouldThrowExceptionWhenInputFilesAreNull() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .build();

        // When / Then
        String expectedMessage = "The bulk import job failed validation with the following checks failing: \n"
                + "The input files must be set to a non-null and non-empty value.";
        assertThatThrownBy(() -> stateMachineExecutor.runJob(myJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    public void shouldOverwriteDefaultConfigurationIfSpecifiedInJob() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .sparkConf("spark.driver.memory", "10g")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .filter(JsonElement::isJsonPrimitive) // Filters out the null reference caused by the null config bucket
                .map(JsonElement::getAsString)
                .filter(s -> s.contains("spark.driver.memory=")))
                .containsExactly("spark.driver.memory=10g");
    }

    @Test
    public void shouldUseDefaultJobIdIfNoneWasPresentInTheJob() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .filter(JsonElement::isJsonPrimitive) // Filters out the null reference caused by the null config bucket
                .map(JsonElement::getAsString)
                .filter(s -> s.contains("spark.driver.memory=")))
                .containsExactly("spark.driver.memory=7g");
    }

    @Test
    public void shouldSetJobIdToUUIDIfNotSetByUser() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .files(Lists.newArrayList("file1.parquet"))
                .tableName("myTable")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonObject jsonJobObject = parsed.getAsJsonObject().getAsJsonObject("job");
        BulkImportJob bulkImportJob = new Gson().fromJson(jsonJobObject, BulkImportJob.class);
        assertThat(bulkImportJob.getId()).isNotNull();
    }

    @Test
    public void shouldPassConfigBucketToSparkArgs() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .map(JsonElement::getAsString))
                .last().isEqualTo("myBucket");
    }

    @Test
    public void shouldUseJobIdAsDriverPodName() {
        // Given
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, new InstanceProperties(), tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        String input = requested.get().getInput();
        JsonElement parsed = new JsonParser().parse(input);
        JsonArray argsArray = parsed.getAsJsonObject().getAsJsonArray("args");
        assertThat(jsonArrayToStream(argsArray)
                .filter(JsonElement::isJsonPrimitive) // Filters out the null reference caused by the null config bucket
                .map(JsonElement::getAsString)
                .filter(s -> s.contains("spark.kubernetes.driver.pod.name=")))
                .containsExactly("spark.kubernetes.driver.pod.name=my-job");
    }

    private Stream<JsonElement> jsonArrayToStream(JsonArray argsArray) {
        List<JsonElement> elementList = new ArrayList<>();
        for (int j = 0; j < argsArray.size(); j++) {
            elementList.add(argsArray.get(j));
        }
        return elementList.stream();
    }
}
