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

# Same with external properties file (URL, bucket name, credentials)
java -jar target/testdata-loader-1.0.jar --containers 10 --docs 100 --properties src/main/resources/s3/cred.json
```

## Preparation

Before executing the code, you need to provide your AWS S3 credentials in the resource folder in
`resources/s3/cred.json` or in any other file. If an external file is used, you mus use the CLI option
`--properties <path to JSON file`.

## Sample output 

### Load Test with AWS S3

```
------- RUNNING with 1 threads ---------
checkRootContainer load-0001 your-bucket-001
Number of existing top level containers: 1
S3ObjectSummary{bucketName='your-bucket-001', key='load-0001/', ...}
------- createContainers START ---------
...
------- createContainers END ---------
* ContainerCreation
* Count   = 10
* Average = 4.967883050894204 msecs
Number of created containers: 10
------- fillContainers START ---------
...
------- fillContainers END ---------
* Total duration = 189136 msecs.
* PdfDocumentCreation
* Count   = 1000
* Average = 5.231360416693275 msecs
* DocumentUpload
* Count   = 1000
* Average = 5.23136315608987 msecs
Number of created documents: 1000
------- FINISHED ---------
```

### Local Minio

```
------- fillContainers END ---------
* Total duration = 69093 msecs.
* PdfDocumentCreation
* Count   = 1000
* Average = 14.2994127249683 msecs
* DocumentUpload
* Count   = 1000
* Average = 14.299421668427374 msecs
Number of created documents: 1000
------- FINISHED ---------
```
