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

import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.local.ReadSplitPoints;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.table.TablePropertyGroup;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Path("/api/sleeper")
public class SleeperPropertiesResource {

    @GET
    @Path("/instance/properties")
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
    @Path("/instance/properties/validate")
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
    @Path("/table/properties")
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
    @Path("/table/properties/validate")
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

    @POST
    @Path("/schema/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ValidationResult validateSchema(String schemaJson) {
        if (schemaJson == null || schemaJson.isBlank()) {
            return new ValidationResult(false, "Schema is empty");
        }
        try {
            new SchemaSerDe().fromJson(schemaJson);
        } catch (RuntimeException e) {
            return new ValidationResult(false, e.getMessage());
        }
        return new ValidationResult(true, null);
    }

    @POST
    @Path("/split-points/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ValidationResult validateSplitPoints(SplitPointsValidationRequest request) {
        if (request == null || request.schema() == null || request.schema().isBlank()) {
            return new ValidationResult(false, "Missing schema");
        }

        Schema schema;
        try {
            schema = new SchemaSerDe().fromJson(request.schema());
        } catch (RuntimeException e) {
            return new ValidationResult(false, "Invalid schema: " + e.getMessage());
        }

        List<String> lines = request.splitPoints() == null ? List.of() : request.splitPoints();
        List<Object> parsed;
        try {
            parsed = ReadSplitPoints.fromString(String.join("\n", lines), schema, false);
        } catch (RuntimeException e) {
            return new ValidationResult(false, e.getMessage());
        }

        try {
            // Delegate ordering / duplicate checks to partition builder
            new PartitionsFromSplitPoints(schema, parsed).construct();
        } catch (IllegalArgumentException e) {
            return new ValidationResult(false, e.getMessage());
        }

        return new ValidationResult(true, null);
    }

    public record Group(String description, List<Property> properties){};
    public record Property(String name, String description, String defaultValue, Boolean isEditable, Boolean isRunCdkDeployWhenChanged){};
    public record ValidationRequest(String name, String value){};
    public record ValidationResult(boolean valid, String reason){};
    public record SplitPointsValidationRequest(String schema, List<String> splitPoints){};
    public record UpdateFailure(
            String message,
            List<InvalidProperty> invalidProperties,
            List<String> nonEditableProperties,
            List<String> cdkDeployRequiredProperties,
            List<String> unknownProperties){};
    public record InvalidProperty(String name, String value, String reason){};

}
