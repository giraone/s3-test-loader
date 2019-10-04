package com.giraone.s3.objectstore.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Intended to provide high level service operations on top of S3.
 */
public class EnhancedObjectStorageService {
    private AmazonS3 objectStorageService;

    public EnhancedObjectStorageService(AmazonS3 objectStorageService) {
        super();
        this.objectStorageService = objectStorageService;
    }

    public AmazonS3 getObjectStorageService() {
        return objectStorageService;
    }

    public PutObjectResult createContainer(String bucketName, String containerName) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("application/directory");
        objectMetadata.setContentLength(0);

        PutObjectRequest objectRequest;
        try (ByteArrayInputStream nullStream = new ByteArrayInputStream(new byte[0])) {
            objectRequest = new PutObjectRequest(bucketName, containerName + "/", nullStream, objectMetadata);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        try {
            return this.objectStorageService.putObject(objectRequest);
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
            return null;
        }
    }

    public PutObjectResult createDocumentFromFile(String bucketName, String objectPath, Map<String, String> userMetadata, File file)
            throws Exception {
        // System.out.println("createDocumentFromFile: " + objectPath);

        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentType("text/plain");
        metaData.setContentLength(file.length());
        metaData.setUserMetadata(userMetadata);

        PutObjectResult result;
        try (FileInputStream in = new FileInputStream(file)) {
            result = this.objectStorageService.putObject(new PutObjectRequest(bucketName, objectPath, in, metaData));
        }

        return result;
    }

    public Map<String, String> readMetaData(String bucketName, String objectPath)
            throws Exception {
        // System.out.println("readMetaData: " + objectPath);

        try (S3Object object = this.objectStorageService.getObject(bucketName, objectPath)) {
            ObjectMetadata objectMetadata = object.getObjectMetadata();
            return objectMetadata.getUserMetadata();
        }
    }
}
