package com.giraone.s3.objectstore.smoketest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.s3.objectstore.authentication.Authenticator;
import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.service.EnhancedObjectStorageService;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Some basic tests for limits in S3 implementations, e.g. meta data size.
 */
public class LimitTesterS3 {

    private static String resourcePath = "s3/cred.json";

    private final boolean TRACE = false;
    private final MetricRegistry metrics = new MetricRegistry();
    private final Timer monitorDocumentCreation = metrics.timer(MetricRegistry.name(LimitTesterS3.class, "DocumentCreation"));

    private ObjectStorageEnvironment env;
    private String bucketName;
    private String containerName;

    private LimitTesterS3() {
        super();
    }

    private void setEnv(ObjectStorageEnvironment env) {
        this.env = env;
    }

    private void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    private void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    private PutObjectResult createContainer(EnhancedObjectStorageService es, String bucketName, String containerName) {
        PutObjectResult result = es.createContainer(bucketName, containerName);
        if (result == null) {
            throw new IllegalStateException("Cannot create container \"" + containerName + "\" in bucket \"" + bucketName + "\"!");
        }
        return result;
    }

    private PutObjectResult createDocumentAndVerifyMetaData(EnhancedObjectStorageService es, String bucketName, String containerName, String documentName, Map<String, String> userMetadataIn) {
        String objectPath = containerName + "/" + documentName;

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonIn;
        try {
            jsonIn = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(userMetadataIn);
        } catch (JsonProcessingException e1) {
            e1.printStackTrace();
            return null;
        }
        if (TRACE) {
            System.out.println(jsonIn);
            System.out.println("Size = " + jsonIn.length());
        }

        PutObjectResult result = createDocument(es, bucketName, objectPath, userMetadataIn);

        Map<String, String> userMetadataOut;
        try {
            userMetadataOut = es.readMetaData(bucketName, objectPath);
        } catch (AmazonS3Exception s3e) {
            System.err.println("AmazonS3Exception AmazonS3Exception AmazonS3Exception");
            s3e.printStackTrace();
            System.err.println(s3e.getRawResponseContent());
            throw new IllegalStateException("Cannot fetch meta data of document \"" + objectPath + "\" in bucket \"" + bucketName + "\"!");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Cannot fetch meta data of document \"" + objectPath + "\" in bucket \"" + bucketName + "\"!");
        }

        String jsonOut;
        try {
            jsonOut = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(userMetadataOut);
        } catch (JsonProcessingException e1) {
            e1.printStackTrace();
            return null;
        }
        if (TRACE) {
            System.out.println(jsonOut);
            System.out.println("Size = " + jsonOut.length());
        }

        if (!mapsAreEquals(userMetadataIn, userMetadataOut)) {
            throw new IllegalStateException("User data differs!");
        }
        return result;
    }

    private PutObjectResult createDocument(EnhancedObjectStorageService es, String bucketName, String objectPath, Map<String, String> userMetadataIn) {
        File file;
        try {
            file = createSampleFile();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Cannot create test file!");
        }

        PutObjectResult result;
        try {
            result = es.createDocumentFromFile(bucketName, objectPath, userMetadataIn, file);
        } catch (AmazonS3Exception s3e) {
            System.err.println("AmazonS3Exception AmazonS3Exception AmazonS3Exception");
            s3e.printStackTrace();
            throw new IllegalStateException("Cannot fetch meta data of document \"" + objectPath + "\" in bucket \"" + bucketName + "\"!");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Cannot create document \"" + objectPath + "\" in bucket \"" + bucketName + "\"! ");
        }
        return result;
    }

    private void initTestContainer(EnhancedObjectStorageService es) {
        AmazonS3 objectStorageService = es.getObjectStorageService();

        Bucket theBucket = null;
        for (Bucket bucket : objectStorageService.listBuckets()) {
            if (bucket.getName().equals(this.bucketName)) {
                theBucket = bucket;
                System.out.println("Found bucket \"" + this.bucketName + "\".");
                break;
            }
        }
        if (theBucket == null) {
            throw new IllegalArgumentException("No bucket \"" + this.bucketName + "\"!");
        }

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(this.bucketName)
                .withDelimiter("/")
                .withMaxKeys(1) // Only 1 to prove existence
                .withPrefix(this.containerName + "/");
        ObjectListing objects = objectStorageService.listObjects(listObjectsRequest);
        boolean found = objects.getObjectSummaries().size() > 0;

        if (!found) {
            this.createContainer(es, bucketName, containerName);
            System.out.println("Container \"" + this.containerName + "\" created.");
        } else {
            System.out.println("Container \"" + this.containerName + "\" found.");
        }
    }

    /**
     * Test how big meta data on objects can be.
     */
    private void testMetaDataLimit(EnhancedObjectStorageService es) {
        initTestContainer(es);

        try {
            Map<String, String> userMetadataIn = new HashMap<>();
            for (int i = 0; i < 43; i++)
            // 42 geht. 43: AmazonS3Exception: Your metadata headers exceed the maximum allowed metadata size
            {
                String key = "userdata-" + String.format("%010d", i);
                String value = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
                System.out.println("Probing with size " + (i * (key + value).length()));
                userMetadataIn.put(key, value);
            }
            String documentName = "test.txt";
            PutObjectResult ret = this.createDocumentAndVerifyMetaData(es, bucketName, containerName, documentName, userMetadataIn);
            if (ret != null) {
                System.out.println("Document \"" + documentName + "\" created. ETag = " + ret.getETag());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
	
/*
# Mit Index 
Duration = 302872 msecs. #Docs = 10000. Avg = 30
DocumentCreation
Count   = 10000
Average = 32.885719227833164 msecs

# Ohne Index
Duration = 293035 msecs. #Docs = 10000. Avg = 29
DocumentCreation
Count   = 10000
Average = 34.009637049888084 msecs
	 */

    /**
     * Prepare test bed for meta data search - create lots of documents with meta data that is indexed
     */
    private void prepareMetaDataSearch(EnhancedObjectStorageService es) {
        initTestContainer(es);

        int startNr = 1000;
        int endNr = 100000;

        String[] status = new String[]{"OPEN", "INWORK", "CLOSED"};
        String[] tags1 = new String[]{"XXS", "XS", "S", "M", "L", "XL", "XXL"};
        String[] tags2 = new String[]{"red", "green", "blue", "yellow", "brown", "magenta", "black", "white"};
        String[] titles = new String[]{"rechnung", "mahnung", "lieferschein", "angebot", "spende"};

        long start = System.currentTimeMillis();
        for (int i = startNr; i < endNr; i++) {
            Map<String, String> userMetadataIn = new HashMap<>();
            userMetadataIn.put("status", status[i % status.length]);
            userMetadataIn.put("tag01", tags1[i % tags1.length]);
            userMetadataIn.put("tag02", tags2[i % tags2.length]);
            userMetadataIn.put("taxyear", Integer.toString(2000 + i % 16));
            userMetadataIn.put("title", titles[i % titles.length]);

            String objectPath = containerName + "/test-" + String.format("%08d", i) + ".txt";

            if (i % 100 == 0) {
                System.out.println(objectPath);
            }

            final Timer.Context context = monitorDocumentCreation.time();
            this.createDocument(es, bucketName, objectPath, userMetadataIn);
            context.stop();
        }

        long end = System.currentTimeMillis();

        System.out.println("Duration = " + (end - start) + " msecs. #Docs = " + (endNr - startNr) + ". Avg = " + (end - start) / (endNr - startNr));
        printMonitorData(monitorDocumentCreation, "DocumentCreation");
    }

    private void testPagingWithMarkerV2(EnhancedObjectStorageService es) {
        AmazonS3 objectStorageService = es.getObjectStorageService();

        initTestContainer(es);

        int pageSize = 100;
        String prefix = this.containerName + "/";
        String marker = "";

        ListObjectsV2Result result;


        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(this.bucketName)
                .withMaxKeys(pageSize)
                .withStartAfter(marker)
                .withPrefix(prefix);
        do {
            System.out.println("Query with continuationToken \"" + request.getContinuationToken() + "\"");
            result = objectStorageService.listObjectsV2(request);
            List<S3ObjectSummary> summaries = result.getObjectSummaries();
            System.out.println(" Next Continuation Token : " + result.getNextContinuationToken());
            System.out.println(" Nr. of objects with marker \"" + marker + "\" = " + summaries.size());

            System.out.println(" first key is \"" + summaries.get(0).getKey() + "\"");

            request.setContinuationToken(result.getNextContinuationToken());

        }
        while (result.isTruncated());
    }

    /**
     * Test paging with marker and max-keys.
     */
    private void testPagingWithMarker(EnhancedObjectStorageService es) {
        AmazonS3 objectStorageService = es.getObjectStorageService();

        initTestContainer(es);

        int pageSize = 100;
        // We want only a certain container, not others
        String prefix = this.containerName + "/";
        // We want only the objects beginning with this name
        String marker = this.containerName + "/test-000008";

        try {
            System.out.println("Listing objects");

            ListObjectsRequest req = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withMarker(marker)
                    .withMaxKeys(pageSize)
                    .withPrefix(prefix);

            ObjectListing listing = null;

            int loop = 0;

            do {
                if (listing == null) {
                    listing = objectStorageService.listObjects(req);
                } else {
                    listing = objectStorageService.listNextBatchOfObjects(listing);
                }

                for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                    System.out.println(" - " + (loop) + " " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
                }

                loop++;
            }
            while (listing.isTruncated());
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
            System.out.println("Caught an AmazonServiceException, " + "which means your request made it "
                    + "to Amazon S3, but was rejected with an error response " + "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " + "which means the client encountered "
                    + "an internal error while trying to communicate" + " with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    //------------------------------------------------------------------------------------------------------

    private static boolean mapsAreEquals(Map<String, String> m1, Map<String, String> m2) {
        for (String k : m1.keySet()) {
            if (!m1.get(k).equals(m2.get(k))) {
                System.err.println("1=>2: " + k + " " + m1.get(k) + " != " + m2.get(k));
                return false;
            }
        }
        for (String k : m2.keySet()) {
            if (!m2.get(k).equals(m1.get(k))) {
                System.err.println("2=>1: " + k + " " + m2.get(k) + " != " + m1.get(k));
                return false;
            }
        }
        return true;
    }

    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    private static void printMonitorData(Timer timer, String name) {
        System.out.println(name);
        System.out.println("Count   = " + timer.getCount());
        System.out.println("Average = " + timer.getMeanRate() + " msecs");
    }

    public static void main(String[] args) throws Exception {

        ObjectStorageEnvironment env = new ObjectStorageEnvironment();
        env.readFromFileOrResource(resourcePath);

        LimitTesterS3 limitTester = new LimitTesterS3();
        limitTester.setEnv(env);
        limitTester.setBucketName(env.getServiceProperties().getBucketForSmokeTest());
        limitTester.setContainerName(env.getServiceProperties().getFolderForSmokeTest());

        AmazonS3 objectStorageService = Authenticator.init(env);
        EnhancedObjectStorageService es = new EnhancedObjectStorageService(objectStorageService);

        limitTester.testMetaDataLimit(es);

        //limitTester.prepareMetaDataSearch(es);

        limitTester.testPagingWithMarkerV2(es);

        limitTester.testPagingWithMarker(es);
    }
}
