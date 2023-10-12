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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Files.newInputStream(Paths.get(splitPointsFile)), StandardCharsets.UTF_8))) {
            String lineFromFile = reader.readLine();
            List<String> lines = new ArrayList<>();
            while (null != lineFromFile) {
                lines.add(lineFromFile);
                lineFromFile = reader.readLine();
            }
            return fromLines(tableProperties.getSchema(), lines, stringsBase64Encoded);
        }
    }

    static List<Object> fromLines(Schema schema, List<String> lines, boolean stringsBase64Encoded) {
        List<Object> splitPoints = new ArrayList<>();
        PrimitiveType rowKey1Type = schema.getRowKeyTypes().get(0);
        for (String line : lines) {
            if (rowKey1Type instanceof IntType) {
                splitPoints.add(Integer.parseInt(line));
            } else if (rowKey1Type instanceof LongType) {
                splitPoints.add(Long.parseLong(line));
            } else if (rowKey1Type instanceof StringType) {
                if (stringsBase64Encoded) {
                    byte[] encodedString = Base64.decodeBase64(line);
                    splitPoints.add(new String(encodedString, StandardCharsets.UTF_8));
                } else {
                    splitPoints.add(line);
                }
            } else if (rowKey1Type instanceof ByteArrayType) {
                splitPoints.add(Base64.decodeBase64(line));
            } else {
                throw new RuntimeException("Unknown key type " + rowKey1Type);
            }
        }
        System.out.println("Read " + splitPoints.size() + " split points from file");
        return splitPoints;
    }
}
