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
package sleeper.api;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import sleeper.api.resources.SleeperPropertiesResource.InvalidProperty;
import sleeper.api.resources.SleeperPropertiesResource.UpdateFailure;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.SleeperPropertyIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class PropertyUpdates {

    private PropertyUpdates() {
    }

    public static <T extends SleeperProperty> void applyOrThrow(
            SleeperProperties<T> properties, Map<String, String> requestedChanges) {

        if (requestedChanges == null || requestedChanges.isEmpty()) {
            return;
        }

        SleeperPropertyIndex<T> index = properties.getPropertiesIndex();

        List<String> unknown = new ArrayList<>();
        List<String> nonEditable = new ArrayList<>();
        List<String> realChangeNames = new ArrayList<>();

        for (Map.Entry<String, String> entry : requestedChanges.entrySet()) {
            String name = entry.getKey();
            String newValue = entry.getValue();
            Optional<T> propertyOpt = index.getByName(name);
            if (propertyOpt.isEmpty()) {
                unknown.add(name);
                continue;
            }
            T property = propertyOpt.get();
            if (!property.isEditable()) {
                nonEditable.add(name);
                continue;
            }
            String oldValue = properties.get(property);
            if (!Objects.equals(oldValue, newValue)) {
                realChangeNames.add(name);
            }
        }

        if (!unknown.isEmpty() || !nonEditable.isEmpty()) {
            throw failure(new UpdateFailure(
                    buildUnknownOrNonEditableMessage(unknown, nonEditable),
                    List.of(), nonEditable, List.of(), unknown));
        }

        for (String name : realChangeNames) {
            properties.set(index.getByName(name).orElseThrow(), requestedChanges.get(name));
        }

        try {
            properties.validate();
        } catch (SleeperPropertiesInvalidException e) {
            List<InvalidProperty> invalid = e.getInvalidValues().entrySet().stream()
                    .map(en -> new InvalidProperty(
                            en.getKey().getPropertyName(),
                            en.getValue(),
                            en.getKey().getDescription()))
                    .toList();
            throw failure(new UpdateFailure(
                    "One or more property values failed validation.",
                    invalid, List.of(), List.of(), List.of()));
        }

        List<String> cdkRequired = realChangeNames.stream()
                .filter(name -> index.getByName(name)
                        .map(SleeperProperty::isRunCdkDeployWhenChanged)
                        .orElse(false))
                .toList();
        if (!cdkRequired.isEmpty()) {
            throw failure(new UpdateFailure(
                    "Changing properties that require a CDK redeploy is not currently supported.",
                    List.of(), List.of(), cdkRequired, List.of()));
        }
    }

    private static String buildUnknownOrNonEditableMessage(List<String> unknown, List<String> nonEditable) {
        StringBuilder sb = new StringBuilder();
        if (!unknown.isEmpty()) {
            sb.append("Unknown property names: ").append(unknown);
        }
        if (!nonEditable.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(". ");
            }
            sb.append("Non-editable properties cannot be changed: ").append(nonEditable);
        }
        return sb.toString();
    }

    private static WebApplicationException failure(UpdateFailure body) {
        return new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(body).build());
    }
}
