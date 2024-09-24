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

import com.facebook.collections.ByteArray;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes a file storing split points to initialise a Sleeper table partition tree.
 */
public class WriteSplitPoints {

    private WriteSplitPoints() {
    }

    /**
     * Writes the split points to a string.
     *
     * @param  splitPoints          the split points
     * @param  stringsBase64Encoded true if string values should be Base64 encoded
     * @return                      the split points file contents
     */
    public static String toString(List<Object> splitPoints, boolean stringsBase64Encoded) {
        StringWriter writer = new StringWriter();
        try {
            writeSplitPoints(splitPoints, writer, stringsBase64Encoded);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return writer.toString();
    }

    /**
     * Writes the split points to a writer.
     *
     * @param  splitPoints          the split points
     * @param  writer               writer
     * @param  stringsBase64Encoded true if string values should be Base64 encoded
     * @throws IOException          if data could not be written to the writer
     */
    public static void writeSplitPoints(List<Object> splitPoints, Writer writer, boolean stringsBase64Encoded) throws IOException {
        for (Object splitPoint : splitPoints) {
            if (splitPoint instanceof ByteArray) {
                writer.write(Base64.encodeBase64String(((ByteArray) splitPoint).getArray()));
            } else if (splitPoint instanceof byte[]) {
                writer.write(Base64.encodeBase64String((byte[]) splitPoint));
            } else if (splitPoint instanceof String) {
                if (stringsBase64Encoded) {
                    writer.write(Base64.encodeBase64String(((String) splitPoint).getBytes(StandardCharsets.UTF_8)));
                } else {
                    writer.write(splitPoint.toString());
                }
            } else {
                writer.write(splitPoint.toString());
            }
            writer.write("\n");
        }
    }

}
