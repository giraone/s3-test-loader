package com.giraone.s3.objectstore.testdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Value object class / model class for the meta data of a Swift "Container".
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContainerMetaData {
    String uuid;
    String type;
    String title;

    public ContainerMetaData() {
        super();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
