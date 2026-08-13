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
package sleeper.api.resources;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.table.TablePropertyGroup;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Path("/api")
public class SleeperPropertiesResource {

    @GET
    @Path("/sleeper/instance/properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Group> getInstanceProperties() {
        Map<PropertyGroup, List<InstanceProperty>> propertiesByGroup = new LinkedHashMap<>();

        List<PropertyGroup> allGroups = InstancePropertyGroup.getAll();
        for (PropertyGroup group : allGroups) {
            propertiesByGroup.put(group, new ArrayList<>());
        }

        List<InstanceProperty> allProperties = InstanceProperty.getAll();
        for (InstanceProperty property : allProperties) {
            propertiesByGroup.get(property.getPropertyGroup()).add(property);
        }

        Map<String, Group> result = new TreeMap<>();
        propertiesByGroup.forEach((group, properties) -> {
            result.put(group.getName(), new Group(
                group.getDescription(),
                properties.stream().map(p -> new Property(
                    p.getPropertyName(),
                    p.getDescription(),
                    p.getDefaultValue(),
                    p.isEditable(),
                    p.isRunCdkDeployWhenChanged()
                )).toList()
            ));
        });

        return result;
    }

    @POST
    @Path("/sleeper/instance/properties/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ValidationResult validateInstanceProperty(ValidationRequest request) {
        if (request == null || request.name() == null) {
            throw new WebApplicationException("Missing property name", Response.Status.BAD_REQUEST);
        }
        InstanceProperty property = InstanceProperty.getAll().stream()
                .filter(p -> p.getPropertyName().equals(request.name()))
                .findFirst()
                .orElseThrow(() -> new WebApplicationException(
                        "Unknown property: " + request.name(), Response.Status.NOT_FOUND));
        String value = request.value() == null ? "" : request.value();
        boolean valid;
        try {
            valid = property.getValidationPredicate().test(value);
        } catch (RuntimeException e) {
            return new ValidationResult(false, e.getMessage());
        }
        return new ValidationResult(valid, valid ? null : "Value did not pass the property's validation predicate");
    }

    @GET
    @Path("/sleeper/table/properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Group> getTableProperties() {
        Map<PropertyGroup, List<TableProperty>> propertiesByGroup = new LinkedHashMap<>();

        List<PropertyGroup> allGroups = TablePropertyGroup.getAll();
        for (PropertyGroup group : allGroups) {
            propertiesByGroup.put(group, new ArrayList<>());
        }

        List<TableProperty> allProperties = TableProperty.getAll();
        for (TableProperty property : allProperties) {
            propertiesByGroup.get(property.getPropertyGroup()).add(property);
        }

        Map<String, Group> result = new TreeMap<>();
        propertiesByGroup.forEach((group, properties) -> {
            result.put(group.getName(), new Group(
                group.getDescription(),
                properties.stream().map(p -> new Property(
                    p.getPropertyName(),
                    p.getDescription(),
                    p.getDefaultValue(),
                    p.isEditable(),
                    p.isRunCdkDeployWhenChanged()
                )).toList()
            ));
        });

        return result;
    }

    @POST
    @Path("/sleeper/table/properties/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ValidationResult validateTableProperty(ValidationRequest request) {
        if (request == null || request.name() == null) {
            throw new WebApplicationException("Missing property name", Response.Status.BAD_REQUEST);
        }
        TableProperty property = TableProperty.getAll().stream()
                .filter(p -> p.getPropertyName().equals(request.name()))
                .findFirst()
                .orElseThrow(() -> new WebApplicationException(
                        "Unknown property: " + request.name(), Response.Status.NOT_FOUND));
        String value = request.value() == null ? "" : request.value();
        boolean valid;
        try {
            valid = property.getValidationPredicate().test(value);
        } catch (RuntimeException e) {
            return new ValidationResult(false, e.getMessage());
        }
        return new ValidationResult(valid, valid ? null : "Value did not pass the property's validation predicate");
    }

    public record Group(String description, List<Property> properties){};
    public record Property(String name, String description, String defaultValue, Boolean isEditable, Boolean isRunCdkDeployWhenChanged){};
    public record ValidationRequest(String name, String value){};
    public record ValidationResult(boolean valid, String reason){};

    public record UpdateFailure(
            String message,
            List<InvalidProperty> invalidProperties,
            List<String> nonEditableProperties,
            List<String> cdkDeployRequiredProperties,
            List<String> unknownProperties){};
    public record InvalidProperty(String name, String value, String reason){};

}
