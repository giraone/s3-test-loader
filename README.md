# Performance and Load Test Tool for S3 Object Stores

A test project for generating containers and documents (PDF), that are stored in S3 using the AWS SDK.
The test client is a Java main program, which can produce folders and documents in parallel threads.

## Code

The most important classes are

- [ObjectStoreTestLoaderS3.java](https://github.com/giraone/s3-test-loader/blob/master/src/main/java/com/giraone/s3/objectstore/testloader/ObjectStoreTestLoaderS3.java) -
  a CLI program for loading test data with some basics *Metrics* based measurements
- [SmokeTester.java](https://github.com/giraone/s3-test-loader/blob/master/src/main/com/giraone/s3/smoketest/SmokeTesterS3.java) -
  shows how to use the the S3 interface and offers some quick functional tests
  
## Build and run

```
mvn package

java -jar target/testdata-loader-1.0.jar --help

# Generates 10 folders with 100 PDF documents in each folder
java -jar target/testdata-loader-1.0.jar --containers 10 --docs 100
```

## Preparation

Before executing the code, you need to provide your AWS S3 credentials in the resource folder.
