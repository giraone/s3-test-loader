package com.giraone.s3.objectstore.testdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

/**
 * Value object class / model class for the meta data of a Swift or S3 "Object" (in our case a document).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DocumentMetaData {

    private String uuid;
    private long time;
    private long counter;
    private String title;
    private HashMap<String, String> metaData;

    public DocumentMetaData() {
        super();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public HashMap<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(HashMap<String, String> metaData) {
        this.metaData = metaData;
    }
}
