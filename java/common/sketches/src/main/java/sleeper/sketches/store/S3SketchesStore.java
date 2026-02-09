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
package sleeper.sketches.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;

import sleeper.core.schema.Schema;
import sleeper.core.util.S3Filename;
import sleeper.sketches.Sketches;
import sleeper.sketches.SketchesSerDe;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class S3SketchesStore implements SketchesStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3SketchesStore.class);

    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;

    public S3SketchesStore(S3Client s3Client, S3TransferManager s3TransferManager) {
        this.s3Client = s3Client;
        this.s3TransferManager = s3TransferManager;
    }

    public static S3SketchesStore createWriteOnly(S3TransferManager s3TransferManager) {
        return new S3SketchesStore(null, s3TransferManager);
    }

    public static S3SketchesStore createReadOnly(S3Client s3Client) {
        return new S3SketchesStore(s3Client, null);
    }

    @Override
    public void saveFileSketches(String filename, Schema schema, Sketches sketches) {
        S3Filename s3Filename = S3Filename.parse(filename);
        BlockingOutputStreamAsyncRequestBody body = BlockingOutputStreamAsyncRequestBody.builder().build();
        Upload upload = s3TransferManager.upload(request -> request
                .putObjectRequest(put -> put
                        .bucket(s3Filename.bucketName())
                        .key(s3Filename.sketchesObjectKey()))
                .requestBody(body));
        SketchesSerDe serDe = new SketchesSerDe(schema);
        try (DataOutputStream out = new DataOutputStream(body.outputStream())) {
            serDe.serialise(sketches, out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        upload.completionFuture().join();
        LOGGER.info("Wrote sketches file to bucket {}, object key {}",
                s3Filename.bucketName(), s3Filename.sketchesObjectKey());
    }

    @Override
    public Sketches loadFileSketches(String filename, Schema schema) {
        LOGGER.info("Loading sketches for file {}", filename);
        S3Filename s3Filename = S3Filename.parse(filename);
        SketchesSerDe serDe = new SketchesSerDe(schema);
        return s3Client.getObject(request -> request
                .bucket(s3Filename.bucketName())
                .key(s3Filename.sketchesObjectKey()),
                (response, inputStream) -> serDe.deserialise(new DataInputStream(inputStream)));
    }

}
