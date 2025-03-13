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
package sleeper.core.testutils.printers;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * A helper class which wraps an output stream. Provides methods for creating a {@link PrintWriter} and calling toString
 * on the output stream.
 */
public class ToStringPrintWriter {

    private final OutputStream outputStream = new ByteArrayOutputStream();

    public PrintStream getPrintStream() {
        return new PrintStream(outputStream, false, StandardCharsets.UTF_8);
    }

    public PrintWriter getPrintWriter() {
        return new PrintWriter(getPrintStream());
    }

    @Override
    public String toString() {
        return outputStream.toString();
    }
}
