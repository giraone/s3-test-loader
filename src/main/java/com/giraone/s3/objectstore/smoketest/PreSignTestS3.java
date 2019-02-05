package com.giraone.s3.objectstore.smoketest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.s3.objectstore.authentication.Authenticator;
import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.service.EnhancedObjectStorageService;

import java.io.*;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test performance of pre-signing
 */
public class PreSignTestS3 {

    //private static String resourcePath = "cred-local-minio.json";
    private static String resourcePath = "s3/cred-aws-test.json";

    private final MetricRegistry metrics = new MetricRegistry();
    private final Timer monitorSigning = metrics.timer(MetricRegistry.name(PreSignTestS3.class, "Signing"));

    private AmazonS3 s3Client;
    private String bucketName;
    private String containerName;

    private PreSignTestS3(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    private void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    private void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    private String presign(S3ObjectSummary s3Object) {

        final String objectPath = s3Object.getKey();
        final Date expiration = new Date(System.currentTimeMillis() + 1000 * 60);
        final URL preSignedUrl = s3Client.generatePresignedUrl(bucketName, objectPath, expiration);
        return preSignedUrl.toExternalForm();
    }

    private void calculatePresignedUrls() {

        int pageSize = 100;
        String prefix = this.containerName + "/";
        String marker = "";

        ListObjectsV2Result result;

        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(this.bucketName)
                .withMaxKeys(pageSize)
                .withStartAfter(marker)
                .withPrefix(prefix);
        do {
            result = s3Client.listObjectsV2(request);
            List<S3ObjectSummary> summaries = result.getObjectSummaries();

            summaries.stream()
                    .filter(s -> s.getKey().endsWith(".pdf"))
                    .forEach(s -> {
                        final Timer.Context context = monitorSigning.time();
                        String url = presign(s);
                        context.stop();
                        System.out.println("Pre-signed URL is \"" + url + "\"");
                    });

            request.setContinuationToken(result.getNextContinuationToken());
        }
        while (result.isTruncated());

        printMonitorData(monitorSigning, "Signing");
    }

    private static void printMonitorData(Timer timer, String name) {
        System.out.println(name);
        System.out.println("Count   = " + timer.getCount());
        System.out.println("Average = " + timer.getMeanRate() + " msecs");
    }

    public static void main(String[] args) throws Exception {

        ObjectStorageEnvironment env = new ObjectStorageEnvironment();
        env.readFromResource(resourcePath);
        AmazonS3 s3Client = Authenticator.init(env);

        PreSignTestS3 test = new PreSignTestS3(s3Client);
        test.setBucketName(env.getServiceProperties().getBucketForLoadTest());
        test.setContainerName(env.getServiceProperties().getFolderForLoadTest());

        test.calculatePresignedUrls();
    }
}
