# Performance and Load Test Tool for S3 Object Stores

A test project for generating containers and documents (PDF), that are stored in S3 using the AWS SDK.
The test client is a Java main program, which can produce folders and documents in parallel threads.

## Code

The most important classes are

- [ObjectStoreTestLoaderS3.java](src/main/com/giraone/s3/testloader/ObjectStoreTestLoaderS3.java) -
  a CLI program for loading test data with some basics *Metrics* based measurements
- [SmokeTester.java](src/main/com/giraone/s3/smoketest/SmokeTesterS3.java) -
  shows how to use the the S3 interface and offers some quick functional tests
  
## Build and run

```
mvn package

java -jar target/testdata-loader-1.0.jar --help

java -jar target/testdata-loader-1.0.jar --containers 10 --docs 100
```


