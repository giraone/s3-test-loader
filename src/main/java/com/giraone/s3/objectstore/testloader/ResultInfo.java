package com.giraone.s3.objectstore.testloader;

public class ResultInfo {
    int numberOfContainers;
    int numberOfDocumentsPerContainer;

    int numberOfContainersCreated;
    int numberOfDocumentsCreated;

    int lastErrorCode;
    String lastError;

    public ResultInfo(int numberOfContainers, int numberOfDocumentsPerContainer) {
        super();

        this.numberOfContainers = numberOfContainers;
        this.numberOfDocumentsPerContainer = numberOfDocumentsPerContainer;

        this.numberOfContainersCreated = 0;
        this.numberOfDocumentsCreated = 0;
    }

    public int getNumberOfContainers() {
        return numberOfContainers;
    }

    public int getNumberOfDocumentsPerContainer() {
        return numberOfDocumentsPerContainer;
    }

    public int getNumberOfContainersCreated() {
        return numberOfContainersCreated;
    }

    public int getNumberOfDocumentsCreated() {
        return numberOfDocumentsCreated;
    }


    public int getLastErrorCode() {
        return lastErrorCode;
    }

    public String getLastError() {
        return lastError;
    }


    public void addContainerResultOk() {
        numberOfContainersCreated++;
    }

    public void addContainerResultError(int code, String fault) {
        lastErrorCode = code;
        lastError = fault;
    }

    public void addDocumentResultOk() {
        numberOfDocumentsCreated++;
    }

    public void addDocumentResultError(int code, String fault) {
        lastErrorCode = code;
        lastError = fault;
    }
}
