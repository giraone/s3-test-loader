package com.giraone.s3.objectstore.testloader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.codahale.metrics.Timer;

import com.giraone.s3.objectstore.authentication.Authenticator;
import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.testdata.DocumentMetaData;
import com.giraone.s3.objectstore.testdata.TestConfig;

public class ObjectStoreTestLoaderS3 extends ObjectStoreTestLoaderBase {

    //private static String resourcePath = "cred-local-minio.json";
    private static String resourcePath = "s3/cred-aws-test.json";

    private ThreadLocal<AmazonS3> objectStorageService = new ThreadLocal<>();

    private ObjectStoreTestLoaderS3() {
        super();
    }

    private AmazonS3 getObjectStorageService() {
        if (this.objectStorageService.get() == null) {
            this.objectStorageService.set(this.init());
        }
        return this.objectStorageService.get();
    }

    private AmazonS3 init() {
        if (this.env == null) {
            throw new IllegalStateException("init() called without valid environment!");
        }
        return Authenticator.init(this.env);
    }

    String createDocument(String rootContainerName, String objectPath, File jsonFile, File pdfFile) {

        DocumentMetaData metaData;
        try {
            metaData = this.parseFile(jsonFile);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // Version with UUID as object name
        // String objectName = metaData.getUuid() + ".pdf";

        // Version with human readable name as object name
        String objectName = metaData.getTitle();

        Map<String, String> objectMetaDataMap = this.buildObjectMetaData(metaData);

        final Timer.Context context = monitorDocumentUpload.time();

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("application/pdf");
        objectMetadata.setContentLength(pdfFile.length());
        objectMetadata.setUserMetadata(objectMetaDataMap);

        String eTag;
        try (FileInputStream in = new FileInputStream(pdfFile)) {
            PutObjectResult result = getObjectStorageService().putObject(
                    new PutObjectRequest(this.testConfig.getBucketName(), prefixWithBucket(rootContainerName + "/" + objectPath + "/" + objectName), in, objectMetadata));
            eTag = result.getETag();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            context.stop();
        }

        return eTag;
    }

    boolean createContainer(int containerIndex) {

        String rootContainer = testConfig.getRootContainerName();
        String containerName = testConfig.getDynamicConfigGenerator().buildContainerName(containerIndex);

        printState(" createContainer START " + containerIndex + " " + containerName);

        final Timer.Context context = monitorContainerCreation.time();

        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("application/directory");
            objectMetadata.setContentLength(0);

            PutObjectRequest objectRequest;
            try (ByteArrayInputStream nullStream = new ByteArrayInputStream(new byte[0])) {
                objectRequest = new PutObjectRequest(testConfig.getBucketName(),
                        prefixWithBucket(rootContainer + "/" + containerName + "/"), nullStream, objectMetadata);
            } catch (IOException e) {
                e.printStackTrace();
                resultInfo.addContainerResultError(-1, e.getMessage());
                return false;
            }

            try {
                PutObjectResult result = getObjectStorageService().putObject(objectRequest);
                if (result.getETag() != null) {
                    resultInfo.addContainerResultOk();
                    return true;
                } else {
                    resultInfo.addContainerResultError(-1, "No ETag returned for container \"" + containerName + "\"");
                    return false;
                }
            } catch (AmazonServiceException ase) {
                resultInfo.addContainerResultError(ase.getStatusCode(), ase.getErrorMessage());
                return false;
            }
        } finally {
            context.stop();
        }
    }

    boolean checkRootContainer(String rootContainerName) {

        System.err.println("checkRootContainer " + rootContainerName + " " + this.testConfig.getBucketName());
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(this.testConfig.getBucketName())
                .withPrefix(prefixWithBucket(rootContainerName + "/"));
        ObjectListing objectListing = this.getObjectStorageService().listObjects(request);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

        System.out.println("Number of existing top level containers: " + objectSummaries.size());

        objectSummaries.forEach(System.out::println);

        final String rootContainerNameWithSlash = prefixWithBucket(rootContainerName + "/");
        S3ObjectSummary anyObjectInContainer = objectSummaries.stream().filter(c -> c.getKey().startsWith(rootContainerNameWithSlash)).findAny().orElse(null);
        return anyObjectInContainer != null;
    }

    private String prefixWithBucket(String path) {
        return prefixWithBucket(testConfig.getBucketName(), path);
    }

    private String prefixWithBucket(String bucketName, String path) {
        if (env.getServiceProperties().isPrefixWithBucketName()) {
            return bucketName + "/" + path;
        } else {
            return path;
        }
    }

    public static void main(String[] args) throws Exception {
        TestConfig testConfig = parseCli(args);
        if (testConfig == null) {
            System.exit(1);
        }

        ObjectStorageEnvironment env = new ObjectStorageEnvironment();
        env.readFromResource(resourcePath);
        System.out.println(env.toJsonString());

        if (testConfig.getBucketName() == null || testConfig.getBucketName().trim().equals("")) {
            testConfig.setBucketName(env.getServiceProperties().getBucketForLoadTest());
        }
        if (testConfig.getRootContainerName() == null || testConfig.getRootContainerName().trim().equals("")) {
            testConfig.setRootContainerName(env.getServiceProperties().getFolderForLoadTest());
        }
        ObjectStoreTestLoaderS3 testLoader = new ObjectStoreTestLoaderS3();
        testLoader.run(testConfig, env);

        printState("FINISHED");
    }
}