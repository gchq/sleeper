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
package sleeper.compaction.status;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class DynamoDBAttributes {

    private DynamoDBAttributes() {
    }

    /**
     * Creates a String attribute. This method abstracts an AWS call to make life easier when upgrading SDK
     *
     * @param str the string to convert
     * @return the AttributeValue
     */
    public static AttributeValue createStringAttribute(String str) {
        return new AttributeValue(str);
    }

    /**
     * Creates a Number attribute. This method abstracts an AWS call to make life easier when upgrading SDK
     *
     * @param number the number to convert
     * @return the AttributeValue
     */
    public static AttributeValue createNumberAttribute(Number number) {
        return new AttributeValue().withN("" + number);
    }

    public static AttributeValue createBinaryAttribute(byte[] bytes) {
        return new AttributeValue().withB(ByteBuffer.wrap(bytes));
    }

    public static String getStringAttribute(Map<String, AttributeValue> item, String name) {
        return getAttribute(item, name, AttributeValue::getS);
    }

    public static String getNumberAttribute(Map<String, AttributeValue> item, String name) {
        return getAttribute(item, name, AttributeValue::getN);
    }

    private static <T> T getAttribute(Map<String, AttributeValue> item, String name, Function<AttributeValue, T> getter) {
        AttributeValue value = item.get(name);
        if (value == null) {
            return null;
        } else {
            return getter.apply(value);
        }
    }

}
