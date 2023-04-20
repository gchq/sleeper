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
package sleeper.clients.util.console.menu;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.UserExitedException;

import java.util.Arrays;
import java.util.List;

public class ChooseOne {

    private final ConsoleOutput out;
    private final ConsoleInput in;

    public ChooseOne(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.in = in;
    }

    @SafeVarargs
    public final <T extends ConsoleChoice> Chosen<T> chooseFrom(T... choices) throws UserExitedException {
        return chooseFrom(Arrays.asList(choices));
    }

    @SafeVarargs
    public final <T extends ConsoleChoice> Chosen<T> chooseWithMessageFrom(String message, T... choices) throws UserExitedException {
        return chooseWithMessageFrom(message, Arrays.asList(choices));
    }

    public <T extends ConsoleChoice> Chosen<T> chooseFrom(List<T> choices) throws UserExitedException {
        return chooseWithMessageFrom("Please select from the below options and hit return:", choices);
    }

    public <T extends ConsoleChoice> Chosen<T> chooseWithMessageFrom(String message, List<T> choices) throws UserExitedException {
        out.println(message);
        out.println("[0] Exit program");
        for (int i = 0; i < choices.size(); i++) {
            out.printf("[%s] %s%n", i + 1, choices.get(i).getDescription());
        }
        out.println();
        String enteredStr = in.promptLine("Input: ");
        try {
            int entered = Integer.parseInt(enteredStr);
            if (entered == 0) {
                throw new UserExitedException();
            }
            return new Chosen<>(enteredStr, choices.get(entered - 1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            return Chosen.nothing(enteredStr);
        }
    }

}
