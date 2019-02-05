package com.giraone.s3.objectstore.authentication;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.config.ServiceProperties;

public class Authenticator {
    private Authenticator() {
    }

    public static AmazonS3 init(ObjectStorageEnvironment env) {

        ServiceProperties serviceProperties = env.getServiceProperties();
        AWSCredentials awsCredentials = new BasicAWSCredentials(serviceProperties.getUserName(), serviceProperties.getPassword());

        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // HTTP/HTTPS Proxy
        if (serviceProperties.isUseProxy()) {
            clientConfiguration.setProtocol(Protocol.HTTP);
            clientConfiguration.setProxyHost("localhost");
            clientConfiguration.setProxyPort(8080);
        }

        return AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                serviceProperties.getServiceEndpoint(),
                                serviceProperties.getRegion()))
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withPathStyleAccessEnabled(true) // virtual-host vs. path-style
                //.withPayloadSigningEnabled(true) // Default=false: Full payload signing can be expensive, if transferring large payloads in a single chunk.
                .withChunkedEncodingDisabled(true) // Default=false: Enabling this option has performance implications since the checksum for the payload will have
                // to be pre-calculated before sending the data.
                .build();
    }
}