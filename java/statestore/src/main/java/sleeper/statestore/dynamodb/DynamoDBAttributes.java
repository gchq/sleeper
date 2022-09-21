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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.nio.ByteBuffer;

public class DynamoDBAttributes {

    private DynamoDBAttributes() {
    }

    /**
     * Creates a String attribute. This method abstracts an AWS call to make life easier when upgrading SDK
     *
     * @param str the string to convert
     * @return the AttributeValue
     */
    static AttributeValue createStringAttribute(String str) {
        return new AttributeValue(str);
    }

    /**
     * Creates a Number attribute. This method abstracts an AWS call to make life easier when upgrading SDK
     *
     * @param number the number to convert
     * @return the AttributeValue
     */
    static AttributeValue createNumberAttribute(Number number) {
        return new AttributeValue().withN("" + number);
    }

    static AttributeValue createBinaryAttribute(byte[] bytes) {
        return new AttributeValue().withB(ByteBuffer.wrap(bytes));
    }

}
