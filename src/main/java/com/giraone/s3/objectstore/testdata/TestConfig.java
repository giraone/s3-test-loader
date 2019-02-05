package com.giraone.s3.objectstore.testdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Overall test configuration
 */
public class TestConfig {
    private String bucketName;
    private String rootContainerName;
    private int numberOfContainers;
    private int numberOfDocumentsPerContainer;

    private int numberOfThreads;

    private DynamicConfigGenerator dynamicConfigGenerator;

    public TestConfig() {
        super();
        this.bucketName = null;
        this.rootContainerName = null;
        this.dynamicConfigGenerator = new DefaultDynamicConfigGenerator();
        this.numberOfContainers = 10;
        this.numberOfDocumentsPerContainer = 100;
        this.numberOfThreads = 1;
    }

    public String getBucketName() {
        return bucketName;
    }


    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }


    public String getRootContainerName() {
        return rootContainerName;
    }

    public void setRootContainerName(String rootContainerName) {
        this.rootContainerName = rootContainerName;
    }

    public int getNumberOfContainers() {
        return numberOfContainers;
    }

    public void setNumberOfContainers(int numberOfContainers) {
        this.numberOfContainers = numberOfContainers;
    }

    public int getNumberOfDocumentsPerContainer() {
        return numberOfDocumentsPerContainer;
    }

    public void setNumberOfDocumentsPerContainer(int numberOfDocumentsPerContainer) {
        this.numberOfDocumentsPerContainer = numberOfDocumentsPerContainer;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public DynamicConfigGenerator getDynamicConfigGenerator() {
        return dynamicConfigGenerator;
    }

    public void setDynamicConfigGenerator(DynamicConfigGenerator dynamicConfigGenerator) {
        this.dynamicConfigGenerator = dynamicConfigGenerator;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return this.getClass().getSimpleName() + this.hashCode();
        }
    }
}