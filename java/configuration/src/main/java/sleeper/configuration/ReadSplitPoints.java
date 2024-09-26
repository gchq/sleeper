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

package sleeper.configuration;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
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

/**
 * Reads a file storing split points to initialise a Sleeper table partition tree.
 */
public class ReadSplitPoints {

    private ReadSplitPoints() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadSplitPoints.class);

    /**
     * Reads the local split points file if set in the table property.
     *
     * @param  tableProperties the table properties
     * @return                 the split points, or an empty list if not set
     * @throws IOException     if the file could not be read
     */
    public static List<Object> readSplitPoints(TableProperties tableProperties) throws IOException {
        if (tableProperties.get(TableProperty.SPLIT_POINTS_FILE) != null) {
            return readSplitPoints(tableProperties,
                    tableProperties.get(TableProperty.SPLIT_POINTS_FILE),
                    tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED));
        } else {
            return List.of();
        }
    }

    /**
     * Reads a local split points file.
     *
     * @param  tableProperties      the table properties
     * @param  splitPointsFile      the path to the file
     * @param  stringsBase64Encoded true if string values are Base64 encoded in the file
     * @return                      the split points
     * @throws IOException          if the file could not be read
     */
    public static List<Object> readSplitPoints(TableProperties tableProperties, String splitPointsFile, boolean stringsBase64Encoded) throws IOException {
        List<Object> splitPoints = fromLines(Files.lines(Paths.get(splitPointsFile)), tableProperties.getSchema(), stringsBase64Encoded);
        LOGGER.info("Read {} split points from file: {}", splitPoints.size(), splitPointsFile);
        return splitPoints;
    }

    /**
     * Reads a split points file held in a string.
     *
     * @param  splitPoints          the split points file contents
     * @param  schema               the Sleeper table schema
     * @param  stringsBase64Encoded true if string values are Base64 encoded in the file
     * @return                      the split points
     */
    public static List<Object> fromString(String splitPoints, Schema schema, boolean stringsBase64Encoded) {
        return fromLines(splitPoints.lines(), schema, stringsBase64Encoded);
    }

    private static List<Object> fromLines(Stream<String> lines, Schema schema, boolean stringsBase64Encoded) {
        PrimitiveType rowKey1Type = schema.getRowKeyTypes().get(0);
        return lines.map(line -> readSplitPoint(line, rowKey1Type, stringsBase64Encoded))
                .collect(Collectors.toUnmodifiableList());
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
