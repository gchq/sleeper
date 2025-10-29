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
package sleeper.clients.images;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

public class FakeSqsLambdaInvocation {

    private static final Gson GSON = new GsonBuilder().create();

    private final List<SqsRecord> Records;

    public FakeSqsLambdaInvocation(List<String> messages) {
        Records = messages.stream().map(SqsRecord::new).toList();
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public record SqsRecord(String body) {
    }
}
