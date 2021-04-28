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

package com.dtstack.flinkx.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.ArrayList;
import java.util.List;

/**
 * a util for connect to AmazonS3
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3Util {

    public static AmazonS3 initS3(S3Config s3Config) {
        if (s3Config != null) {
            Regions clientRegion = Regions.fromName(s3Config.getRegion());
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(s3Config.getAccessKey(), s3Config.getSecretKey())));
            if (null != s3Config.getEndpoint() && !"".equals(s3Config.getEndpoint().trim())) {
                builder = builder.withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(s3Config.getEndpoint(), clientRegion.getName()));
            } else {
                builder = builder.withRegion(clientRegion.getName());
            }

            return builder.build();
        } else {
            // todo: throw exception
            return null;
        }
    }


    public static List<String> listObjects(AmazonS3 s3Client, String bucketName) {
        List<String> objects = new ArrayList<>(64);
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withMaxKeys(64);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                objects.add(objectSummary.getKey());
            }
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (result.isTruncated());
        return objects;
    }

    public static List<String> listObjectsByPrefix(AmazonS3 s3Client, String bucketName, String prefix) {
        List<String> objects = new ArrayList<>(64);
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withMaxKeys(64).withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                objects.add(objectSummary.getKey());
            }
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (result.isTruncated());
        return objects;
    }

    public static boolean doesObjectExist(AmazonS3 s3Client, String bucketName, String object) {
        return s3Client.doesObjectExist(bucketName, object);
    }

    /**
     * get S3SimpleObject{@link S3SimpleObject} from AWS S3
     *
     * @param s3Client
     * @param bucketName
     * @param object
     * @return
     */
    public static S3SimpleObject getS3SimpleObject(AmazonS3 s3Client, String bucketName, String object) {
        S3Object s3Object = s3Client.getObject(bucketName, object);
        return new S3SimpleObject(object, s3Object.getObjectMetadata().getContentLength());
    }


}
