package com.giraone.s3.objectstore.testdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Value object class / model class for the meta data of a Swift "Account" (tenant).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMetaData {
    String uuid;
    String displayName;

    public AccountMetaData() {
        super();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }
}
