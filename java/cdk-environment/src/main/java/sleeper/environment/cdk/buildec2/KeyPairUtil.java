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
package sleeper.environment.cdk.buildec2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;

public class KeyPairUtil {

    private KeyPairUtil() {
        // Prevent instantiation
    }

    public static KeyPair generate() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not generate a keypair", e);
        }
    }

    public static void writePrivateToFile(KeyPair pair, String fileName) {
        try {
            Path path = Paths.get(fileName);
            Files.deleteIfExists(path);
            Files.createFile(path, PosixFilePermissions.asFileAttribute(
                    Collections.singleton(PosixFilePermission.OWNER_WRITE)));
            Files.write(path, Collections.singletonList(KeyPairUtil.privatePem(pair)));
            Files.setPosixFilePermissions(path, Collections.singleton(PosixFilePermission.OWNER_READ));
        } catch (IOException e) {
            throw new RuntimeException("Could not write private key", e);
        }
    }

    public static String publicBase64(KeyPair pair) {
        return Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
    }

    public static String privatePem(KeyPair pair) {
        String base64 = Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded());
        return "-----BEGIN RSA PRIVATE KEY-----\n" + addNewLines(base64) + "-----END RSA PRIVATE KEY-----\n";
    }

    private static String addNewLines(String base64) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < base64.length(); i += 64) {
            builder.append(base64, i, Math.min(i + 64, base64.length()));
            builder.append('\n');
        }
        return builder.toString();
    }
}
