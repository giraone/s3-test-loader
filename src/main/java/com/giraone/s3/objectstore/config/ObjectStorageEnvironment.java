package com.giraone.s3.objectstore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.s3.common.TextFileReader;

import java.io.File;

/**
 * Value object for credentials and information about the Object Storage,
 */
public class ObjectStorageEnvironment {
    private ServiceProperties serviceProperties;

    public ServiceProperties getServiceProperties() {
        return serviceProperties;
    }

    public void readFromFileOrResource(String path) throws Exception {

        if (path.startsWith("res:")) {
            this.serviceProperties = (ServiceProperties) TextFileReader
                    .readJsonFileFromResource(path.substring(4), ServiceProperties.class);
        } else {
            this.serviceProperties = (ServiceProperties) TextFileReader
                    .readJsonFileFromPath(new File(path), ServiceProperties.class);
        }
    }

    public String toJsonString() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this.serviceProperties);
    }
}