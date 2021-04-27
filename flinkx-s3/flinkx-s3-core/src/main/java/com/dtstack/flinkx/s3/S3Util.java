package com.dtstack.flinkx.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.ArrayList;
import java.util.List;

public class S3Util {

    public static AmazonS3 initS3(S3Config s3Config){
        if(s3Config != null){
            Regions clientRegion = Regions.fromName(s3Config.getRegion());
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(s3Config.getAccessKey(), s3Config.getSecretKey())));
            if(null != s3Config.getEndpoint() && !"".equals(s3Config.getEndpoint().trim())){
                builder = builder.withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(s3Config.getEndpoint(), clientRegion.getName()));
            }else{
                builder = builder.withRegion(clientRegion.getName());
            }

            return  builder.build();
        }else {
            // todo: throw exception
            return null;
        }
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucketName, String prefix) {
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
        return s3Client.doesObjectExist(bucketName,object);
    }








}
