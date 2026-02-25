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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import software.amazon.awssdk.transfer.s3.progress.TransferListener.Context.BytesTransferred;
import software.amazon.awssdk.transfer.s3.progress.TransferListener.Context.TransferComplete;
import software.amazon.awssdk.transfer.s3.progress.TransferListener.Context.TransferFailed;
import software.amazon.awssdk.transfer.s3.progress.TransferListener.Context.TransferInitiated;
import software.amazon.lambda.powertools.cloudformation.AbstractCustomResourceHandler;
import software.amazon.lambda.powertools.cloudformation.Response;
import software.amazon.lambda.powertools.cloudformation.Response.Status;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

public class CopyJarLambda extends AbstractCustomResourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyJarLambda.class);

    private final S3TransferManager s3TransferManager;
    private final HttpClient httpClient;
    private final Consumer<HttpRequest.Builder> httpRequestConfig;

    public CopyJarLambda() {
        this(builder().s3TransferManager(S3TransferManager.create()));
    }

    public CopyJarLambda(Builder builder) {
        s3TransferManager = builder.s3TransferManager;
        httpClient = builder.httpClient;
        httpRequestConfig = builder.httpRequestConfig;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Response create(CloudFormationCustomResourceEvent event, Context context) {
        return copyJar(event);
    }

    @Override
    protected Response update(CloudFormationCustomResourceEvent event, Context context) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'update'");
    }

    @Override
    protected Response delete(CloudFormationCustomResourceEvent event, Context context) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    private Response copyJar(CloudFormationCustomResourceEvent event) {
        Map<String, Object> properties = event.getResourceProperties();
        String source = (String) properties.get("source");
        String bucket = (String) properties.get("bucket");
        String key = (String) properties.get("key");

        LOGGER.info("Initiating copy from {} to {}/{}", source, bucket, key);
        try (DownloadResponse response = openDownload(source)) {
            Upload upload = s3TransferManager.upload(UploadRequest.builder()
                    .requestBody(AsyncRequestBody.fromInputStream(response.stream(), response.contentLength(), ForkJoinPool.commonPool()))
                    .putObjectRequest(PutObjectRequest.builder().bucket(bucket).key(key).build())
                    .addTransferListener(new CopyJarListener())
                    .build());
            CompletedUpload completed = upload.completionFuture().join();
            return Response.builder()
                    .status(Status.SUCCESS)
                    .physicalResourceId(bucket + "/" + key)
                    .value(Map.of("versionId", completed.response().versionId()))
                    .build();
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to download jar", e);
            return Response.builder()
                    .status(Status.FAILED)
                    .physicalResourceId(bucket + "/" + key)
                    .reason("Failed to download jar: " + e.getMessage())
                    .build();
        }
    }

    private DownloadResponse openDownload(String url) {
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().uri(URI.create(url));
            httpRequestConfig.accept(requestBuilder);
            HttpResponse<InputStream> response = httpClient.send(
                    requestBuilder.build(),
                    BodyHandlers.ofInputStream());
            LOGGER.info("Response: {}", response);
            LOGGER.info("Response headers: {}", response.headers());
            if (response.statusCode() < 200 || response.statusCode() > 299) {
                throw new RuntimeException("Unsuccessful status code: " + response.statusCode());
            }
            String contentLengthHeader = response.headers()
                    .firstValue("Content-Length")
                    .orElseThrow(() -> new RuntimeException("No content length header found"));
            long contentLength = Long.parseLong(contentLengthHeader);
            return new DownloadResponse(contentLength, response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private record DownloadResponse(long contentLength, InputStream stream) implements AutoCloseable {

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    public static class CopyJarListener implements TransferListener {

        @Override
        public void transferInitiated(TransferInitiated context) {
            LOGGER.info("Transfer initiated: {}", context.request());
        }

        @Override
        public void bytesTransferred(BytesTransferred context) {
            LOGGER.info("Progress: {}", context.progressSnapshot());
        }

        @Override
        public void transferComplete(TransferComplete context) {
            LOGGER.info("Progress: {}", context.progressSnapshot());
            LOGGER.info("Transfer complete: {}", context.completedTransfer());
        }

        @Override
        public void transferFailed(TransferFailed context) {
            LOGGER.error("Transfer failed", context.exception());
        }
    }

    public static class Builder {
        private S3TransferManager s3TransferManager;
        private HttpClient httpClient = HttpClient.newHttpClient();
        private Consumer<HttpRequest.Builder> httpRequestConfig = builder -> {
        };

        private Builder() {
        }

        public Builder s3TransferManager(S3TransferManager s3TransferManager) {
            this.s3TransferManager = s3TransferManager;
            return this;
        }

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder httpRequestConfig(Consumer<HttpRequest.Builder> httpRequestConfig) {
            this.httpRequestConfig = httpRequestConfig;
            return this;
        }

        public CopyJarLambda build() {
            return new CopyJarLambda(this);
        }
    }

}
