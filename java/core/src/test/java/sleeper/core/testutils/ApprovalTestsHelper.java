/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.testutils;

import org.approvaltests.core.Options;
import org.approvaltests.namer.TemplatedCustomNamer;

/**
 * Helpers for setting the names of files used by ApprovalTests.
 */
public class ApprovalTestsHelper {

    private ApprovalTestsHelper() {
    }

    /**
     * Creates options to use an approved file with the given base name, in the same directory as the test class. This
     * must be called from within the test method, as the directory is found from the stack trace.
     *
     * @param  baseName the base name of the file, without the approved/received suffix or file extension
     * @return          the options
     */
    public static Options namedApprovalFile(String baseName) {
        return new Options().forFile().withNamer(TemplatedCustomNamer.create(
                "{test_source_directory}/{approvals_subdirectory}/" + baseName + ".{approved_or_received}.{file_extension}"));
    }

    /**
     * Creates options to use an approved file with the given base name and extension, in the same directory as the
     * test class. This must be called from within the test method, as the directory is found from the stack trace.
     *
     * @param  baseName  the base name of the file, without the approved/received suffix or file extension
     * @param  extension the file extension
     * @return           the options
     */
    public static Options namedApprovalFile(String baseName, String extension) {
        return namedApprovalFile(baseName).forFile().withExtension(extension);
    }
}
