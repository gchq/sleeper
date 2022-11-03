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
package sleeper.console.menu;

import sleeper.console.UserExitedException;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Chosen<T extends ConsoleChoice> {

    private final String entered;
    private final T choice;
    private final boolean exit;

    public Chosen(String entered, T choice) {
        this(entered, choice, false);
    }

    private Chosen(String entered, T choice, boolean exit) {
        this.entered = entered;
        this.choice = choice;
        this.exit = exit;
    }

    public static <T extends ConsoleChoice> Chosen<T> exit(String entered) {
        return new Chosen<>(entered, null, true);
    }

    public static <T extends ConsoleChoice> Chosen<T> nothing(String entered) {
        return new Chosen<>(entered, null);
    }

    public String getEntered() {
        return entered;
    }

    public Optional<T> getChoice() {
        return Optional.ofNullable(choice);
    }

    public boolean isExit() {
        return exit;
    }

    public T chooseUntilChoiceFound(Supplier<Chosen<T>> chooseAgain) throws UserExitedException {
        if (exit) {
            throw new UserExitedException();
        } else if (choice != null) {
            return choice;
        } else {
            return chooseAgain.get().chooseUntilChoiceFound(chooseAgain);
        }
    }

    public Chosen<T> chooseUntilSomethingEntered(Supplier<Chosen<T>> chooseAgain) throws UserExitedException {
        if (exit) {
            throw new UserExitedException();
        } else if ("".equals(entered)) {
            return chooseAgain.get().chooseUntilSomethingEntered(chooseAgain);
        } else {
            return this;
        }
    }

    public void ifEnteredNonChoiceValue(Consumer<String> doWithEnteredValue) {
        if (!exit && choice == null) {
            doWithEnteredValue.accept(entered);
        }
    }
}
