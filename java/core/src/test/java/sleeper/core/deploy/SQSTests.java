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
package sleeper.core.deploy;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.CdkDefinedInstanceProperty;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SQSTests {

    @Test
    void shouldValidateThatAllTheQueuePropertiesArePartOfTheListDefinitions() throws Exception {
        assertThat(SqsQueues.QUEUE_URL_PROPERTIES).containsAll(grabAllValidProperties("queue.url"));
    }

    @Test
    void shouldValidateThatAllTheDeadLetterQueuePropertiesArePartOfTheListDefinitions() throws Exception {
        assertThat(SqsQueues.DEAD_LETTER_QUEUE_URL_PROPERTIES).containsAll(grabAllValidProperties("dlq.url"));
    }

    private List<CdkDefinedInstanceProperty> grabAllValidProperties(String nameDef) throws Exception {
        List<CdkDefinedInstanceProperty> outList = new ArrayList<CdkDefinedInstanceProperty>();

        Field[] interfaceFields = CdkDefinedInstanceProperty.class.getFields();
        for (Field f : interfaceFields) {
            CdkDefinedInstanceProperty present = (CdkDefinedInstanceProperty) f.get(this);
            if (present.getPropertyName().contains(nameDef)) {
                outList.add(present);
            }
        }
        return outList;
    }
}
