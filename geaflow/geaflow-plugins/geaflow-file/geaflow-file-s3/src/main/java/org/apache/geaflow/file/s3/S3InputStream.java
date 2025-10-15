/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.file.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3InputStream extends InputStream {

    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String key;
    private long curPos;
    private byte[] buffer;
    private int bufferPos;
    private final int minChunkSize;
    private final long fileLen;
    private boolean closed;

    public S3InputStream(S3AsyncClient s3Client, String bucketName, String key, int minChunkSize)
        throws IOException {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.curPos = 0;
        this.buffer = new byte[0];
        this.bufferPos = 0;
        this.minChunkSize = minChunkSize;
        this.fileLen = checkFileExistAndGetFileLength();
        this.closed = false;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException(
                "s3 input stream closed, bucket name: " + bucketName + ", file path" + key);
        }

        if (curPos >= fileLen) {
            return -1;
        }

        if (bufferPos >= buffer.length) {
            long newPos = curPos + this.buffer.length;
            if (newPos < fileLen) {
                fetchChunk(newPos, minChunkSize);
            } else {
                return -1;
            }
        }
        int byteRead = buffer[bufferPos] & 0xFF;
        bufferPos++;
        return byteRead;
    }

    @Override
    public int read(@NotNull byte[] buffer) throws IOException {
        return readInner(buffer, 0, buffer.length);
    }

    @Override
    public int read(@NotNull byte[] buffer, int offset, int length) throws IOException {
        return readInner(buffer, offset, length);
    }

    private int readInner(@NotNull byte[] buffer, int offset, int length) throws IOException {
        int bytesRead = 0;
        while (bytesRead < length) {
            if (bufferPos >= this.buffer.length) {
                long newPos = curPos + this.buffer.length;
                if (newPos < fileLen) {
                    fetchChunk(newPos, Integer.max(length - bytesRead, minChunkSize));
                } else {
                    return bytesRead == 0 ? -1 : bytesRead;
                }
            }
            int bytesToCopy = Math.min(length - bytesRead, this.buffer.length - bufferPos);
            System.arraycopy(this.buffer, bufferPos, buffer, offset + bytesRead, bytesToCopy);
            bufferPos += bytesToCopy;
            bytesRead += bytesToCopy;
        }
        return bytesRead;
    }

    private void fetchChunk(long position, int chunkSize) throws IOException {
        long endPosition = position + chunkSize - 1;
        endPosition = Math.min(endPosition, fileLen - 1);

        String range = "bytes=" + position + "-" + endPosition;
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(key)
            .checksumMode(ChecksumMode.UNKNOWN_TO_SDK_VERSION).range(range).build();

        CompletableFuture<byte[]> future = s3Client.getObject(request,
            AsyncResponseTransformer.toBytes()).thenApply(BytesWrapper::asByteArray);

        try {
            buffer = future.get();
            bufferPos = 0;
            curPos = position;
        } catch (Throwable e) {
            throw new IOException(
                "Failed to fetch chunk, bucket name: " + bucketName + ", file path: " + key
                    + ", position: " + position, e);
        }
    }

    private long checkFileExistAndGetFileLength() throws IOException {
        CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(
            ListObjectsV2Request.builder().bucket(bucketName).prefix(key).build());
        ListObjectsV2Response response;

        try {
            response = future.get();
        } catch (Throwable e) {
            throw new IOException(
                "Failed to get file status, bucket name: " + bucketName + ", file path: " + key, e);
        }

        if (response.contents().isEmpty()) {
            throw new IOException(
                "File not found, bucket name: " + bucketName + ", file path: " + key);
        }

        S3Object s3Object = response.contents().get(0);

        if (!s3Object.key().equals(key)) {
            throw new IOException(
                "File not found, bucket name: " + bucketName + ", file path: " + key);
        }
        return s3Object.size();
    }
}

