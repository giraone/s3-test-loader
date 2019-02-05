package com.giraone.s3.objectstore.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model class for the S3 oder OpenStack SWIFT properties and credentials (JSON).
 */
public class ServiceProperties {

    @JsonProperty("service_endpoint")
    private String serviceEndpoint;

    @JsonProperty("region")
    private String region;

    @JsonProperty("username")
    private String userName;

    @JsonProperty("password")
    private String password;

    @JsonProperty("bucket_for_smoke_test")
    private String bucketForSmokeTest = "test";

    @JsonProperty("bucket_for_load_test")
    private String bucketForLoadTest = "load";

    @JsonProperty("folder_for_smoke_test")
    private String folderForSmokeTest = "test";

    @JsonProperty("folder_for_load_test")
    private String folderForLoadTest = "load";

    @JsonProperty("prefix_with_bucket_name")
    private boolean prefixWithBucketName;

    @JsonProperty("use_proxy")
    private boolean useProxy;

    @JsonProperty("create_bucket")
    private boolean createBucket;

    public ServiceProperties() {
        super();
    }

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getBucketForSmokeTest() {
        return bucketForSmokeTest;
    }

    public String getBucketForLoadTest() {
        return bucketForLoadTest;
    }

    public String getFolderForSmokeTest() {
        return folderForSmokeTest;
    }

    public String getFolderForLoadTest() {
        return folderForLoadTest;
    }

    public boolean isPrefixWithBucketName() {
        return prefixWithBucketName;
    }

    public boolean isUseProxy() {
        return useProxy;
    }

    public boolean isCreateBucket() {
        return createBucket;
    }
}