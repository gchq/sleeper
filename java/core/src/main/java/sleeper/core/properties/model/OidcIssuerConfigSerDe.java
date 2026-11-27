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
package sleeper.core.properties.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serialises OIDC issuer configurations to and from the JSON held in a Sleeper configuration property.
 */
public class OidcIssuerConfigSerDe {

    private OidcIssuerConfigSerDe() {
    }

    /**
     * Reads OIDC issuer configurations from the JSON held in a configuration property. An unset or empty value means
     * no issuers are configured.
     *
     * @param  json                     the JSON array of issuer configurations
     * @return                          the issuer configurations, in the order they were declared
     * @throws IllegalArgumentException if the value is not valid
     */
    public static List<OidcIssuerConfig> readList(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        JsonArray array = parseArray(json);
        List<OidcIssuerConfig> issuers = new ArrayList<>();
        Set<String> names = new LinkedHashSet<>();
        Set<String> issuerIds = new LinkedHashSet<>();
        for (JsonElement element : array) {
            OidcIssuerConfig issuer = readIssuer(element);
            if (issuer.name().strip().equalsIgnoreCase(OidcIssuerConfig.COGNITO_ISSUER_NAME)) {
                throw new IllegalArgumentException("OIDC issuers cannot be named \"" + issuer.name() + "\", as the name \"" +
                        OidcIssuerConfig.COGNITO_ISSUER_NAME + "\" is reserved for the Cognito user pool Sleeper can create.");
            }
            if (!names.add(issuer.name())) {
                throw new IllegalArgumentException("OIDC issuers must each have a unique name, found more than one named \"" + issuer.name() + "\".");
            }
            if (!issuerIds.add(issuer.issuer())) {
                throw new IllegalArgumentException("OIDC issuers must each have a unique issuer identifier, found more than one for \"" + issuer.issuer() + "\".");
            }
            issuers.add(issuer);
        }
        return List.copyOf(issuers);
    }

    /**
     * Writes OIDC issuer configurations as the JSON held in a configuration property.
     *
     * @param  issuers the issuer configurations
     * @return         the JSON array
     */
    public static String toJson(List<OidcIssuerConfig> issuers) {
        JsonArray array = new JsonArray();
        for (OidcIssuerConfig issuer : issuers) {
            array.add(writeIssuer(issuer));
        }
        return array.toString();
    }

    private static JsonArray parseArray(String json) {
        JsonElement root;
        try {
            root = JsonParser.parseString(json);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("OIDC issuers must be valid JSON: " + e.getMessage(), e);
        }
        if (!root.isJsonArray()) {
            throw new IllegalArgumentException("OIDC issuers must be a JSON array of issuer configurations.");
        }
        return root.getAsJsonArray();
    }

    private static OidcIssuerConfig readIssuer(JsonElement element) {
        if (!element.isJsonObject()) {
            throw new IllegalArgumentException("Each OIDC issuer must be a JSON object.");
        }
        JsonObject object = element.getAsJsonObject();
        OidcIssuerConfig.Builder builder = OidcIssuerConfig.builder()
                .name(readString(object, "name"))
                .issuer(readString(object, "issuer"))
                .audiences(readStringList(object, "audiences"))
                .jwksUri(readString(object, "jwksUri"))
                .publicKey(readKey(object, "publicKey"))
                .tokenEndpoint(readString(object, "tokenEndpoint"))
                .authEndpoint(readString(object, "authEndpoint"))
                .logoutEndpoint(readString(object, "logoutEndpoint"))
                .clientId(readString(object, "clientId"))
                .requiredClaims(readStringMap(object, "requiredClaims"));
        String principalClaim = readString(object, "principalClaim");
        if (principalClaim != null) {
            builder.principalClaim(principalClaim);
        }
        Integer clockSkewSeconds = readInt(object, "clockSkewSeconds");
        if (clockSkewSeconds != null) {
            builder.clockSkewSeconds(clockSkewSeconds);
        }
        return builder.build();
    }

    private static JsonObject writeIssuer(OidcIssuerConfig issuer) {
        JsonObject object = new JsonObject();
        object.addProperty("name", issuer.name());
        object.addProperty("issuer", issuer.issuer());
        JsonArray audiences = new JsonArray();
        issuer.audiences().forEach(audiences::add);
        object.add("audiences", audiences);
        addIfSet(object, "jwksUri", issuer.jwksUri());
        addKeyIfSet(object, issuer.publicKey());
        addIfSet(object, "tokenEndpoint", issuer.tokenEndpoint());
        addIfSet(object, "authEndpoint", issuer.authEndpoint());
        addIfSet(object, "logoutEndpoint", issuer.logoutEndpoint());
        addIfSet(object, "clientId", issuer.clientId());
        if (!issuer.requiredClaims().isEmpty()) {
            JsonObject requiredClaims = new JsonObject();
            issuer.requiredClaims().forEach(requiredClaims::addProperty);
            object.add("requiredClaims", requiredClaims);
        }
        object.addProperty("principalClaim", issuer.principalClaim());
        object.addProperty("clockSkewSeconds", issuer.clockSkewSeconds());
        return object;
    }

    /**
     * Reads an issuer's signing keys, which may be written as a JSON Web Key Set rather than as text.
     *
     * @param  object the issuer
     * @param  field  the field holding the keys
     * @return        the keys, or null if the issuer does not hold its own
     */
    private static String readKey(JsonObject object, String field) {
        JsonElement element = object.get(field);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (element.isJsonObject()) {
            checkKeysCanBeVerifiedLocally(element.getAsJsonObject());
            return element.toString();
        }
        return readString(object, field);
    }

    /**
     * Checks that keys held in the configuration are ones the API can verify a token against. Keys held here are read
     * as RSA, so an issuer signing with anything else has to be reachable for its keys to be fetched instead.
     *
     * @param  keySet                   the issuer's keys, as a JSON Web Key Set
     * @throws IllegalArgumentException if the API could not verify a token against the keys
     */
    private static void checkKeysCanBeVerifiedLocally(JsonObject keySet) {
        JsonElement keys = keySet.get("keys");
        if (keys == null || !keys.isJsonArray()) {
            throw new IllegalArgumentException("OIDC issuer publicKey must be a JSON Web Key Set, holding keys.");
        }
        if (keys.getAsJsonArray().isEmpty()) {
            throw new IllegalArgumentException("OIDC issuer publicKey must hold at least one key, or no token from " +
                    "the issuer could be verified.");
        }
        for (JsonElement key : keys.getAsJsonArray()) {
            JsonElement keyType = key.isJsonObject() ? key.getAsJsonObject().get("kty") : null;
            if (keyType == null || !keyType.isJsonPrimitive() || !"RSA".equals(keyType.getAsString())) {
                throw new IllegalArgumentException("OIDC issuer publicKey must hold RSA keys, as a key held in the " +
                        "configuration is read as RSA. Set jwksUri instead, to fetch the issuer's keys from it.");
            }
        }
    }

    /**
     * Writes an issuer's signing keys back in the form they were given in.
     *
     * @param object    the issuer
     * @param publicKey the keys, or null if the issuer does not hold its own
     */
    private static void addKeyIfSet(JsonObject object, String publicKey) {
        if (publicKey == null) {
            return;
        }
        try {
            JsonElement parsed = JsonParser.parseString(publicKey);
            if (parsed.isJsonObject()) {
                object.add("publicKey", parsed);
                return;
            }
        } catch (JsonSyntaxException e) {
            // Not a key set, so it is text such as a PEM encoded key.
        }
        object.addProperty("publicKey", publicKey);
    }

    private static void addIfSet(JsonObject object, String field, String value) {
        if (value != null) {
            object.addProperty(field, value);
        }
    }

    private static String readString(JsonObject object, String field) {
        JsonElement element = object.get(field);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException("OIDC issuer " + field + " must be a string.");
        }
        return element.getAsString();
    }

    private static List<String> readStringList(JsonObject object, String field) {
        JsonElement element = object.get(field);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonArray()) {
            throw new IllegalArgumentException("OIDC issuer " + field + " must be an array of strings.");
        }
        List<String> values = new ArrayList<>();
        for (JsonElement entry : element.getAsJsonArray()) {
            if (!entry.isJsonPrimitive() || !entry.getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("OIDC issuer " + field + " must be an array of strings.");
            }
            values.add(entry.getAsString());
        }
        return values;
    }

    private static Map<String, String> readStringMap(JsonObject object, String field) {
        JsonElement element = object.get(field);
        if (element == null || element.isJsonNull()) {
            return Map.of();
        }
        if (!element.isJsonObject()) {
            throw new IllegalArgumentException("OIDC issuer " + field + " must be an object of strings.");
        }
        Map<String, String> values = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
            JsonElement value = entry.getValue();
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("OIDC issuer " + field + " must be an object of strings.");
            }
            values.put(entry.getKey(), value.getAsString());
        }
        return values;
    }

    private static Integer readInt(JsonObject object, String field) {
        JsonElement element = object.get(field);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException("OIDC issuer " + field + " must be a number.");
        }
        try {
            return Integer.parseInt(element.getAsString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("OIDC issuer " + field + " must be a whole number, found: "
                    + element.getAsString(), e);
        }
    }
}
