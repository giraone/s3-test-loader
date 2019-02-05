package com.giraone.s3.objectstore.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.giraone.s3.common.ResourceReader;

/**
 * Value object for credentials and information about the Object Storage,
 */
public class ObjectStorageEnvironment {
    private ServiceProperties serviceProperties;

    public ServiceProperties getServiceProperties() {
        return serviceProperties;
    }

    public void readFromResource(String resourcePath) throws Exception {
        this.serviceProperties = (ServiceProperties) ResourceReader
                .readJsonFileFromResource(resourcePath, ServiceProperties.class);
    }

    public String toJsonString() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this.serviceProperties);
    }
	
	/*
    public static void main(String[] args) throws Exception
    {
    	ObjectStorageEnvironment env = new ObjectStorageEnvironment();
    	env.parse("...");
    	System.out.println(env.toJsonString());
    }
    */
}