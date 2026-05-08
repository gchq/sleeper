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
package sleeper.clients.util.console;

import java.io.Console;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Scanner;

public class ConsoleInput {

    private final Console console;
    private final PrintStream out;
    private final Scanner scanner;

    public ConsoleInput(Console console, PrintStream out, Scanner scanner) {
        this.console = console;
        this.out = Objects.requireNonNull(out, "out must not be null");
        this.scanner = Objects.requireNonNull(scanner, "scanner must not be null");
    }

    public static ConsoleInput stdIn() {
        return new ConsoleInput(System.console(), System.out, new Scanner(System.in, StandardCharsets.UTF_8));
    }

    public String promptLine(String prompt) {
        if (console == null) {
            out.print(prompt);
            return scanner.nextLine();
        } else {
            return console.readLine(prompt);
        }
    }

    public String promptPassword(String prompt) {
        if (console == null) {
            out.print(prompt);
            return scanner.nextLine();
        } else {
            return new String(console.readPassword(prompt));
        }
    }

    public void waitForLine() {
        if (console == null) {
            scanner.nextLine();
        } else {
            console.readLine();
        }
    }
}
