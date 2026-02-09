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
package sleeper.environment.cdk.buildec2;

import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
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
            throw new IllegalStateException("Could not generate a keypair", e);
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
            throw new IllegalStateException("Could not write private key", e);
        }
    }

    public static String publicBase64(KeyPair pair) {
        return Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
    }

    public static String privatePem(KeyPair pair) throws IOException {
        StringWriter stringWriter = new StringWriter();
        JcaPEMWriter w = new JcaPEMWriter(stringWriter);
        w.writeObject(pair);
        w.flush();
        return stringWriter.toString();
    }

    public static KeyPair readPrivatePem(String pem) throws IOException {
        return readPrivatePem(new StringReader(pem));
    }

    public static KeyPair readPrivatePem(Reader reader) throws IOException {
        PEMParser parser = new PEMParser(reader);
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        return converter.getKeyPair((PEMKeyPair) parser.readObject());
    }
}
