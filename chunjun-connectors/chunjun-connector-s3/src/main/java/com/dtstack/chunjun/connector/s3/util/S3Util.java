/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.s3.util;

import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.util.GsonUtil;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class S3Util {

    public static AmazonS3 getS3Client(S3Config s3Config) {
        if (s3Config != null) {
            if (StringUtils.isNotBlank(s3Config.getRegion())) {
                Regions clientRegion = Regions.fromName(s3Config.getRegion());
                AmazonS3ClientBuilder builder =
                        AmazonS3ClientBuilder.standard()
                                .withCredentials(
                                        new AWSStaticCredentialsProvider(
                                                new BasicAWSCredentials(
                                                        s3Config.getAccessKey(),
                                                        s3Config.getSecretKey())));
                if (null != s3Config.getEndpoint() && !"".equals(s3Config.getEndpoint().trim())) {
                    builder =
                            builder.withEndpointConfiguration(
                                    new AwsClientBuilder.EndpointConfiguration(
                                            s3Config.getEndpoint(), clientRegion.getName()));
                } else {
                    builder = builder.withRegion(clientRegion.getName());
                }
                // 禁用 Bucket 名称注入到 endpoint 前缀
                if (s3Config.isDisableBucketNameInEndpoint()) {
                    builder = builder.withPathStyleAccessEnabled(true);
                }

                return builder.build();
            } else {
                BasicAWSCredentials cred =
                        new BasicAWSCredentials(s3Config.getAccessKey(), s3Config.getSecretKey());
                ClientConfiguration ccfg = new ClientConfiguration();
                if (StringUtils.isBlank(s3Config.getProtocol())
                        || "HTTP".equals(s3Config.getProtocol())) {
                    ccfg.setProtocol(Protocol.HTTP);
                } else {
                    ccfg.setProtocol(Protocol.HTTPS);
                }
                AmazonS3Client client = new AmazonS3Client(cred, ccfg);
                client.setEndpoint(s3Config.getEndpoint());
                // 禁用 Bucket 名称注入到 endpoint 前缀
                if (s3Config.isDisableBucketNameInEndpoint()) {
                    client.setS3ClientOptions(
                            S3ClientOptions.builder().setPathStyleAccess(true).build());
                }
                return client;
            }
        } else {
            // todo: throw exception
            return null;
        }
    }

    public static PutObjectResult putStringObject(
            AmazonS3 s3Client, String bucketName, String key, String content) {
        return s3Client.putObject(bucketName, key, content);
    }

    public static List<String> listObjectsKeyByPrefix(
            AmazonS3 s3Client, String bucketName, String prefix, int fetchSize, String regex) {
        List<String> objects = new ArrayList<>(fetchSize);
        ListObjectsV2Request req =
                new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(fetchSize);
        if (StringUtils.isNotBlank(prefix)) {
            req.setPrefix(prefix);
        }
        // 定义正则表达式
        Pattern pattern = null;
        if (StringUtils.isNotBlank(regex)) {
            pattern = Pattern.compile(regex);
        }

        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                // 如果对象键与正则表达式匹配，则进行相应处理
                if (pattern != null
                        && !pattern.matcher(FilenameUtils.getName(objectSummary.getKey())).find()) {
                    continue;
                }
                objects.add(objectSummary.getKey());
            }
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
            if (log.isDebugEnabled()) {
                if (objects.size() > 1024) {
                    log.debug(
                            "nextToken {}, result.isTruncated {}, objectsize {}",
                            token,
                            result.isTruncated(),
                            objects.size());
                } else {
                    log.debug(
                            "nextToken {}, result.isTruncated {}, objects {}",
                            token,
                            result.isTruncated(),
                            GsonUtil.GSON.toJson(objects));
                }
            }
        } while (result.isTruncated());
        return objects;
    }

    public static List<String> listObjectsByv1(
            AmazonS3 s3Client, String bucketName, String prefix, int fetchSize) {
        List<String> objects = new ArrayList<>(fetchSize);

        ListObjectsRequest req = new ListObjectsRequest(bucketName, prefix, null, null, fetchSize);
        ObjectListing ol;
        do {
            ol = s3Client.listObjects(req);

            for (S3ObjectSummary os : ol.getObjectSummaries()) {
                objects.add(os.getKey());
            }

            if (ol.isTruncated()) {
                // next page
                String marker = ol.getNextMarker();
                if (StringUtils.isNotBlank(marker)) {
                    req.setMarker(marker);
                    if (log.isDebugEnabled()) {
                        if (objects.size() > 1024) {
                            log.debug(
                                    "nextToken {}, result.isTruncated {}, objectsSize {}",
                                    marker,
                                    true,
                                    objects.size());
                        } else {
                            log.debug(
                                    "nextToken {}, result.isTruncated {}, objects {}",
                                    marker,
                                    true,
                                    GsonUtil.GSON.toJson(objects));
                        }
                    }
                } else {
                    log.warn("Warning: missing NextMarker when IsTruncated");
                }
            }
        } while (ol.isTruncated());
        return objects;
    }

    public static boolean doesObjectExist(AmazonS3 s3Client, String bucketName, String object) {
        return s3Client.doesObjectExist(bucketName, object);
    }

    /**
     * get S3SimpleObject{@link S3SimpleObject} from AWS S3
     *
     * @param object
     * @return
     */
    public static S3SimpleObject getS3SimpleObject(String object) {
        return new S3SimpleObject(object);
    }

    public static void deleteObject(AmazonS3 s3Client, String bucketName, String object) {
        s3Client.deleteObject(bucketName, object);
    }

    public static void deleteObjects(AmazonS3 s3Client, String bucketName, String[] keys) {
        if (keys.length > 0) {
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
            s3Client.deleteObjects(request);
        }
    }

    public static void closeS3(AmazonS3 amazonS3) {
        if (amazonS3 != null) {
            amazonS3.shutdown();
            amazonS3 = null;
        }
    }

    public static String initiateMultipartUploadAndGetId(
            AmazonS3 s3Client, String bucketName, String object) {
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(bucketName, object);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        return initResponse.getUploadId();
    }

    public static PartETag uploadPart(
            AmazonS3 s3Client,
            String bucketName,
            String object,
            String uploadId,
            int partNumber,
            byte[] data) {
        InputStream inputStream = new ByteArrayInputStream(data);

        UploadPartRequest uploadRequest =
                new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(object)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(inputStream)
                        .withPartSize(data.length);
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        return uploadResult.getPartETag();
    }

    public static void completeMultipartUpload(
            AmazonS3 s3Client,
            String bucketName,
            String object,
            String uploadId,
            List<PartETag> partETags) {
        CompleteMultipartUploadRequest compRequest =
                new CompleteMultipartUploadRequest(bucketName, object, uploadId, partETags);
        s3Client.completeMultipartUpload(compRequest);
    }

    public static void abortMultipartUpload(
            AmazonS3 s3Client, String bucketName, String object, String uploadId) {
        s3Client.abortMultipartUpload(
                new AbortMultipartUploadRequest(bucketName, object, uploadId));
    }

    public static long getFileSize(AmazonS3 s3Client, String bucketName, String keyName) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, keyName);
        return s3Client.getObject(getObjectRequest).getObjectMetadata().getInstanceLength();
    }
}
