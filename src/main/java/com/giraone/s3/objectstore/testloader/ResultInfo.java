package com.giraone.s3.objectstore.testloader;

public class ResultInfo {

    private int numberOfContainers;
    private int numberOfDocumentsPerContainer;

    private int numberOfContainersCreated;
    private int numberOfDocumentsCreated;

    private int lastErrorCode;
    private String lastError;

    ResultInfo(int numberOfContainers, int numberOfDocumentsPerContainer) {
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


    void addContainerResultOk() {
        numberOfContainersCreated++;
    }

    void addContainerResultError(int code, String fault) {
        lastErrorCode = code;
        lastError = fault;
    }

    void addDocumentResultOk() {
        numberOfDocumentsCreated++;
    }

    void addDocumentResultError(int code, String fault) {
        lastErrorCode = code;
        lastError = fault;
    }
}
