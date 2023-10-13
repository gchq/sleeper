/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.configuration;

import org.apache.commons.codec.binary.Base64;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadSplitPoints {

    private ReadSplitPoints() {
    }

    public static List<Object> readSplitPoints(TableProperties tableProperties) throws IOException {
        if (tableProperties.get(TableProperty.SPLIT_POINTS_FILE) != null) {
            return readSplitPoints(tableProperties,
                    tableProperties.get(TableProperty.SPLIT_POINTS_FILE),
                    tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED));
        } else {
            return List.of();
        }
    }

    public static List<Object> readSplitPoints(TableProperties tableProperties, String splitPointsFile, boolean stringsBase64Encoded) throws IOException {
        return fromLines(Files.lines(Paths.get(splitPointsFile)), tableProperties.getSchema(), stringsBase64Encoded);
    }

    public static List<Object> fromString(String splitPoints, Schema schema, boolean stringsBase64Encoded) {
        return fromLines(splitPoints.lines(), schema, stringsBase64Encoded);
    }

    private static List<Object> fromLines(Stream<String> lines, Schema schema, boolean stringsBase64Encoded) {
        PrimitiveType rowKey1Type = schema.getRowKeyTypes().get(0);
        List<Object> splitPoints = lines
                .map(line -> readSplitPoint(line, rowKey1Type, stringsBase64Encoded))
                .collect(Collectors.toUnmodifiableList());
        System.out.println("Read " + splitPoints.size() + " split points from file");
        return splitPoints;
    }

    private static Object readSplitPoint(String line, PrimitiveType rowKeyType, boolean stringsBase64Encoded) {
        if (rowKeyType instanceof IntType) {
            return Integer.parseInt(line);
        } else if (rowKeyType instanceof LongType) {
            return Long.parseLong(line);
        } else if (rowKeyType instanceof StringType) {
            if (stringsBase64Encoded) {
                byte[] encodedString = Base64.decodeBase64(line);
                return new String(encodedString, StandardCharsets.UTF_8);
            } else {
                return line;
            }
        } else if (rowKeyType instanceof ByteArrayType) {
            return Base64.decodeBase64(line);
        } else {
            throw new RuntimeException("Unknown key type " + rowKeyType);
        }
    }
}
