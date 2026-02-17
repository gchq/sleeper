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
package sleeper.cdk;

import org.jetbrains.annotations.NotNull;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.constructs.Construct;

public class SleeperTable extends NestedStack {

    static final String JOB_ID_AND_UPDATE = "JobIdAndUpdate";
    static final String UPDATE_TIME = "UpdateTime";
    static final String EXPIRY_DATE = "ExpiryDate";

    public SleeperTable(@NotNull Construct scope, @NotNull String id) {
        super(scope, id);
    }

    /**
     * Declares a SleeperTable as a root level stack.
     *
     * @param  scope        the scope to add the SleeperTable to, usually an App or Stage
     * @param  id           the stack ID
     * @param  stackProps   configuration of the stack
     * @param  sleeperProps configuration to deploy the instance
     * @return              the stack
     */
    public static Stack createAsRootStack(Construct scope, String id, StackProps stackProps, SleeperInstanceProps sleeperProps) {
        Stack stack = new Stack(scope, id, stackProps);
        return stack;
    }

    public Table createSleeperTable(String tableName, String tableId) {
        return Table.Builder
                .create(this, "SleeperTable")
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(tableId)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(JOB_ID_AND_UPDATE)
                        .type(AttributeType.STRING)
                        .build())
                .timeToLiveAttribute(EXPIRY_DATE)
                .build();
    }
}
