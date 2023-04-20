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
package sleeper.clients.status.report.filestatus;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * An interface the represents a method of presenting the status of files in
 * Sleeper to a user.
 */
public interface FileStatusReporter {

    void report(FileStatus fileStatusReport, boolean verbose);

    static String asString(
            Function<PrintStream, FileStatusReporter> getReporter, FileStatus fileStatusReport, boolean verbose)
            throws UnsupportedEncodingException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        getReporter.apply(new PrintStream(os, false, StandardCharsets.UTF_8.displayName()))
                .report(fileStatusReport, verbose);
        return os.toString(StandardCharsets.UTF_8.displayName());
    }
}
