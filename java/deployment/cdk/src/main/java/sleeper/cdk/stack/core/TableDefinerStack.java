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
package sleeper.cdk.stack.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.cdk.SleeperTable;
import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.schema.Schema;
import sleeper.core.util.EnvironmentUtils;

import java.util.Map;

/**
 * The table definer stack is used to create, update and delete Sleeper tables using a custom resource.
 */
@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class TableDefinerStack extends NestedStack {

    private Provider tableDefinerProvider;

    public TableDefinerStack(
            Construct scope, String id, InstanceProperties instanceProperties, SleeperInstanceArtefacts artefacts) {
        super(scope, id);

        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "table-definer");

        IFunction tableDefinerLambda = lambdaCode.buildFunction(LambdaHandler.TABLE_DEFINER, "TableDefinerLambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .description("Lambda for creating, updating and deleting Sleeper tables")
                .logGroup(LoggingStack.createLogGroup(this, LogGroupRef.TABLE_DEFINER, instanceProperties)));

        tableDefinerProvider = Provider.Builder.create(this, "TableDefinerProvider")
                .onEventHandler(tableDefinerLambda)
                .logGroup(LoggingStack.createLogGroup(this, LogGroupRef.TABLE_DEFINER_PROVIDER, instanceProperties))
                .build();

        Utils.addTags(this, instanceProperties);
    }

    public CustomResource createTableDefinerResource(String propertiesString) {
        return CustomResource.Builder.create(this, "TableDefinerProperties")
                .resourceType("Custom::SleeperTable")
                .properties(Map.of("tableProperties", propertiesString,
                        "splitPoints", ""))
                .serviceToken(tableDefinerProvider.getServiceToken())
                .build();

    }

    public CustomResource createSleeperTable(Schema schema) {
        return SleeperTable.Builder.create(this, "TableDefinerProperties")
                .instanceId("test-instance")
                .tableName("table-name")
                .schema(schema)
                .buildResource(tableDefinerProvider);
    }

    public Provider getTableDefinerProvider() {
        return tableDefinerProvider;
    }

    /**
     * Declares an SleeperDefinerStack as a root level stack.
     *
     * @param  scope      the scope to add the stack to, usually an App or Stage
     * @param  id         the stack ID
     * @param  stackProps configuration of the stack
     * @return            the stack
     */
    public static Stack createAsRootStack(Construct scope, String id, StackProps stackProps) {
        return new Stack(scope, id, stackProps);
    } //TODO Remove this method
}
