package com.giraone.s3.objectstore.smoketest;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.giraone.s3.objectstore.authentication.Authenticator;
import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.config.ServiceProperties;
import com.giraone.s3.objectstore.testdata.TestConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * Minio Experience
 * - The region "default" is needed
 * - Folders need / at the end
 * - The object key must include the bucket name as a prefix with bucketName/objectKey - NOT IN NEWER VERSIONS!
 * - Minio uses Base64 encoded content MD5 digest
 * - Minio ListObject does not list directory/folder
 * - Some feature work only, if .withChunkedEncodingDisabled(true) is used.
 * - Signature V4 should work with path style
 *
 * AWS S3 Experience
 * - Works out-of-the-box
 */
public class SmokeTesterS3 {

    private AmazonS3 s3Client;
    private ServiceProperties serviceProperties;
    private String bucketName;
    private String folderName;

    private SmokeTesterS3(ServiceProperties serviceProperties, AmazonS3 s3Client) {
        this.serviceProperties = serviceProperties;
        this.bucketName = serviceProperties.getBucketForSmokeTest();
        this.folderName = serviceProperties.getFolderForSmokeTest();
        this.s3Client = s3Client;
    }

    private void testAll() {
        long now = System.currentTimeMillis();
        String folderKey = this.folderName;
        String testObjectsPrefix = folderKey + "/text";
        String objectKey1 = testObjectsPrefix + "1-" + now + ".txt";
        String objectKey2 = testObjectsPrefix + "2-" + now + ".txt";

        // Bucket configuration stuff

        showBucketVersioningConfiguration(); // minio: A header you provided implies functionality that is not implemented
        showBucketTaggingConfiguration();
        showBucketAcls();
        // changeBucketsAcls(); // minio: Invalid AccessControlList: missing an S3Owner
        showBucketMetaData();

        // What is in the bucket?
        listSomeObjects("/" /* prefix */);
        countPrefixedObjectsOfBucket(testObjectsPrefix + "/", 10000);
        countPrefixedObjectsOfBucketOldVersion(testObjectsPrefix + "/", 10000);
        countAllObjectsOfBucket(10000);
        countAllObjectsOfBucketOldVersion(10000);

        // New objects
        createNewFolderObject(folderKey);
        createNewObjectFromFile(objectKey1);
        fetchContentAndMetaDataOfObject(objectKey1);
        fetchContentAndMetaDataOfObjectWithPresignedUrl(objectKey1);

        // Show the changes
        listSomeObjects(testObjectsPrefix);
        countPrefixedObjectsOfBucket(testObjectsPrefix, 10000);

        // Copy/Update stuff
        createCopyOfExistingObject(objectKey1, objectKey2);
        createNewVersionOfExistingObject(objectKey1);
        listVersionsOfExistingObject(objectKey1, false);
        listVersionsOfExistingObject(objectKey1, true);

        int pageSize = 100;
        int maxCount = 10000;
        listKeysWithPagingAndNextBatchOf(testObjectsPrefix, pageSize, maxCount, true);
        listKeysWithPagingAndContinuationToken(testObjectsPrefix, pageSize, maxCount, true);

        // Delete all test objects
        deleteTestObjects(testObjectsPrefix);
    }

    public static void run(TestConfig testConfig) throws Exception {

        ObjectStorageEnvironment env = new ObjectStorageEnvironment();
        env.readFromFileOrResource(testConfig.getPropertiesPath());

        AmazonS3 s3Client = Authenticator.init(env);
        ServiceProperties serviceProperties = env.getServiceProperties();

        System.out.println("Initialized with S3AccountOwner = \"" + s3Client.getS3AccountOwner() + "\"");

        SmokeTesterS3 tester = new SmokeTesterS3(serviceProperties, s3Client);
        List<String> bucketNames = tester.listAllBuckets();

        if (bucketNames.isEmpty() || !bucketNames.contains(tester.bucketName)) {
            System.out.println("No bucket \"" + tester.bucketName + "\"");
            if (serviceProperties.isCreateBucket()) {
                System.out.println("Creating \"" + tester.bucketName + "\"");
                tester.createBucket();
            }
        } else {
            System.out.println(bucketNames.size() + " buckets exist. Bucket to be used is \"" + tester.bucketName + "\"");
        }

        tester.testAll();
    }

    public static void main(String[] args) throws Exception {

       TestConfig testConfig = new TestConfig();
       run(testConfig);
    }

    private void createBucket() {
        printTestCase("Create new bucket \"" + bucketName + "\"...");
        Bucket bucket = s3Client.createBucket(bucketName);
        System.out.println("Bucket \"" + bucket.getName() + "\" created. Owner=" + bucket.getOwner() + ", CreationDate=" + bucket.getCreationDate());
    }

    private List<String> listAllBuckets() {
        List<String> ret = new ArrayList<>();
        try {
            printTestCase("List buckets ...");
            for (Bucket bucket : s3Client.listBuckets()) {
                System.out.println(" - " + bucket.getName() + " CreationDate=" + bucket.getCreationDate());
                ret.add(bucket.getName());
            }
        } catch (Exception e) {
            handleException(e);
        }
        return ret;
    }

    private void showBucketTaggingConfiguration() {
        try {
            printTestCase("Show bucket tagging");

            BucketTaggingConfiguration tagConfiguration = s3Client.getBucketTaggingConfiguration(bucketName);
            System.out.println(" - tag size was " + tagConfiguration.getAllTagSets().size());

            for (TagSet tagSet : tagConfiguration.getAllTagSets()) {
                Map<String, String> tags = tagSet.getAllTags();
                for (Map.Entry<String, String> entry : tags.entrySet()) {
                    System.out.format(" - Tag %-20s : %s%n", entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void showBucketVersioningConfiguration() {
        try {

            BucketVersioningConfiguration conf = s3Client.getBucketVersioningConfiguration(bucketName);
            System.out.println(" - was " + conf.getStatus());

            if (!BucketVersioningConfiguration.ENABLED.equals(conf.getStatus())) {
                BucketVersioningConfiguration configuration = new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED);
                SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest(bucketName, configuration);
                s3Client.setBucketVersioningConfiguration(request);

                conf = s3Client.getBucketVersioningConfiguration(bucketName);
                System.out.println(" - is now " + conf.getStatus());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void showBucketAcls() {
        try {
            printTestCase("Display bucket's ACLs");

            AccessControlList aclList = s3Client.getBucketAcl(bucketName);
            System.out.println(" - Owner=" + aclList.getOwner());
            for (Grant grant : aclList.getGrantsAsList()) {
                Grantee grantee = grant.getGrantee();
                System.out.println(" - Grantee " + grantee.getTypeIdentifier() + ":"
                        + grantee.getIdentifier() + " has permission=" + grant.getPermission());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void changeBucketsAcls() {
        try {
            printTestCase("Change bucket's ACLs - add one more");

            AccessControlList aclList = s3Client.getBucketAcl(bucketName);
            System.out.println(" - ACL original size = " + aclList.getGrantsAsList().size());

            Grantee newGrantee = new CanonicalGrantee(bucketName);
            newGrantee.setIdentifier(this.serviceProperties.getUserName() + "2");

            Permission permission = Permission.Read;
            aclList.grantPermission(newGrantee, permission);

            s3Client.setBucketAcl(bucketName, aclList);

            aclList = s3Client.getBucketAcl(bucketName);
            System.out.println(" - ACL changed size = " + aclList.getGrantsAsList().size());

            printTestCase("Change bucket's ACLs - remove new one");

            aclList.revokeAllPermissions(newGrantee);
            s3Client.setBucketAcl(bucketName, aclList);

            aclList = s3Client.getBucketAcl(bucketName);
            System.out.println(" - ACL revert size = " + aclList.getGrantsAsList().size());
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void showBucketMetaData() {
        try {
            printTestCase("Show bucket meta-data");
            // T.B.D.
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void listSomeObjects(String prefix) {
        try {
            printTestCase("Listing up to 10 sample object keys of bucket \"" + bucketName + "\"");

            ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefixWithBucket(prefix))
                    //.withEncodingType("url")
                    .withMaxKeys(20);

            ObjectListing objects = s3Client.listObjects(request);
            int max = 10;
            for (S3ObjectSummary summary : objects.getObjectSummaries()) {
                System.out.println(summary.getKey() + "   " + summary.getOwner());
                if (max-- < 0) {
                    System.out.println("There are more then 10 objects. Rest is skipped!");
                    break;
                }
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private int countPrefixedObjectsOfBucket(String prefix, int limit) {
        int count = 0;

        try {
            printTestCase("Counting all objects of bucket \"" + bucketName + "\" with prefix \"" + prefix + "\" using paging (ContinuationToken)");

            int pageSize = 1000;

            final ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(prefixWithBucket(prefix))
                    .withMaxKeys(pageSize);
            ListObjectsV2Result listing;

            do {
                listing = s3Client.listObjectsV2(request);
                // count += listing.getKeyCount(); // Does not work with EMC ECS - always 0
                count += listing.getObjectSummaries().size();
                request.setContinuationToken(listing.getNextContinuationToken());
            } while (listing.isTruncated() && count < limit);

            if (limit > 0 && count >= limit)
                System.out.println("Number of objects in bucket \"" + bucketName + "\" with prefix \"" + prefix + "\" > " + limit);
            else
                System.out.println("Number of objects in bucket \"" + bucketName + "\" with prefix \"" + prefix + "\" = " + count);
        } catch (Exception e) {
            handleException(e);
        }

        return count;
    }

    private int countPrefixedObjectsOfBucketOldVersion(String prefix, int limit) {
        int count = 0;

        try {
            printTestCase("Counting all objects with prefix \"" + prefix + "\" using paging (listNextBatchOfObjects)");

            int pageSize = 1000;

            final ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefixWithBucket(prefix))
                    .withMaxKeys(pageSize);

            ObjectListing listing = null;

            do {
                if (listing == null) {
                    listing = s3Client.listObjects(request);
                } else {
                    listing = s3Client.listNextBatchOfObjects(listing);
                }

                listing = s3Client.listObjects(request);
                count += listing.getObjectSummaries().size();
            } while (listing.isTruncated() && count < limit);

            if (limit > 0 && count >= limit)
                System.out.println("Number of objects in bucket \"" + bucketName + "\" with prefix \"" + prefix + "\" > " + limit);
            else
                System.out.println("Number of objects in bucket \"" + bucketName + "\" with prefix \"" + prefix + "\" = " + count);
        } catch (Exception e) {
            handleException(e);
        }

        return count;
    }

    private long countAllObjectsOfBucket(int limit) {
        return countPrefixedObjectsOfBucket("", limit);
    }

    private long countAllObjectsOfBucketOldVersion(int limit) {
        return countPrefixedObjectsOfBucketOldVersion("", limit);
    }

    private void createNewFolderObject(String folderKey) {
        try {
            printTestCase("Create a new folder object \"" + bucketName + "/" + folderKey + "/");

            HashMap<String, String> userMetadata = new HashMap<>();
            userMetadata.put("document-type", "folder");
            userMetadata.put("document-date", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));

            ObjectMetadata objectMetaData = new ObjectMetadata();
            objectMetaData.setContentType("application/directory");
            objectMetaData.setContentLength(0);
            objectMetaData.setUserMetadata(userMetadata);

            try (ByteArrayInputStream in = new ByteArrayInputStream(new byte[0])) {
                PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName, prefixWithBucket(folderKey + "/"), in, objectMetaData));
                System.out.println("Created folder object \"" + folderKey + "\" ETag = \"" + result.getETag());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void createNewObjectFromFile(String objectKey) {
        try {
            printTestCase("Uploading a new object \"" + objectKey + "\" to S3 from a file");
            File file = createSampleFile();

            //PutObjectResult result = s3Client.putObject(bucketName, prefixWithBucket(objectKey), file);
            //System.out.println("Created object \"" + objectKey + "\" ETag = \"" + result.getETag());

            HashMap<String, String> userMetadata = new HashMap<>();
            userMetadata.put("document-type", "rechnung");
            userMetadata.put("document-status", "first");
            userMetadata.put("document-date", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));

            ObjectMetadata objectMetaData = new ObjectMetadata();
            objectMetaData.setContentType("text/plain");
            objectMetaData.setContentLength(file.length());
            objectMetaData.setContentEncoding("UTF-8");
            String md5 = buildMd5HashBase64String(new FileInputStream(file));
            objectMetaData.setContentMD5(md5);
            objectMetaData.setUserMetadata(userMetadata);

            try (FileInputStream in = new FileInputStream(file)) {
                PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName, prefixWithBucket(objectKey), in, objectMetaData));
                System.out.println("Created object \"" + objectKey + "\" with MD5 = \"" + md5 + "\" ETag = \"" + result.getETag());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void fetchContentAndMetaDataOfObject(String objectKey) {
        printTestCase("Downloading an object directly");
        try {
            String objectPath = prefixWithBucket(objectKey);
            System.out.println("Content of object \"" + objectPath + "\" is ...");
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectPath));

            System.out.println("Content of objects using pre-signed URL is ...");
            try (InputStream in = object.getObjectContent()) {
                String contentAsString2 = downloadTextContent(in, "UTF-8");
                System.out.println(contentAsString2);
                System.out.println();
            }

            System.out.println("Meta data of object is ...");
            ObjectMetadata objectMetaData = object.getObjectMetadata();

            System.out.format("%-30s: %s%n", "getContentType", object.getObjectMetadata().getContentType());
            System.out.format("%-30s: %s%n", "getContentLength", object.getObjectMetadata().getContentLength());
            System.out.format("%-30s: %s%n", "getContentEncoding", object.getObjectMetadata().getContentEncoding());
            System.out.format("%-30s: %s%n", "getContentDisposition", object.getObjectMetadata().getContentDisposition());
            System.out.format("%-30s: %s%n", "getContentMD5", object.getObjectMetadata().getContentMD5());
            System.out.format("%-30s: %s%n", "getCacheControl", object.getObjectMetadata().getCacheControl());
            System.out.format("%-30s: %s%n", "getETag", object.getObjectMetadata().getETag());
            System.out.format("%-30s: %s%n", "getLastModified", object.getObjectMetadata().getLastModified());
            System.out.format("%-30s: %s%n", "getExpirationTime", object.getObjectMetadata().getExpirationTime());
            System.out.format("%-30s: %s%n", "getHttpExpiresDate", object.getObjectMetadata().getHttpExpiresDate());
            System.out.format("%-30s: %s%n", "getExpirationTimeRuleId", object.getObjectMetadata().getExpirationTimeRuleId());
            System.out.format("%-30s: %s%n", "getInstanceLength", object.getObjectMetadata().getInstanceLength());
            System.out.format("%-30s: %s%n", "getPartCount", object.getObjectMetadata().getPartCount());
            System.out.format("%-30s: %s%n", "getOngoingRestore", object.getObjectMetadata().getOngoingRestore());
            System.out.format("%-30s: %s%n", "getRestoreExpirationTime", object.getObjectMetadata().getRestoreExpirationTime());
            System.out.format("%-30s: %s%n", "getVersionId", object.getObjectMetadata().getVersionId());

            Map<String, String> userMetadata = objectMetaData.getUserMetadata();
            for (String userDataKey : userMetadata.keySet()) {
                System.out.format("USER %-25s: %s%n", userDataKey, userMetadata.get(userDataKey));
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    void fetchContentAndMetaDataOfObjectWithPresignedUrl(String objectKey) {
        try {
            printTestCase("Downloading an object with presigned URL");

            String objectPath = prefixWithBucket(objectKey);
            System.out.println("Content of object \"" + objectPath + "\" is ...");
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectPath));

            Date expiration = new Date(System.currentTimeMillis() + 1000 * 60);
            URL preSignedUrl = s3Client.generatePresignedUrl(bucketName, objectPath, expiration);
            String url = preSignedUrl.toExternalForm();
            System.out.println("Pre-signed URL is \"" + url + "\"");

            System.out.println("Content of objects using pre-signed URL is ...");
            String contentAsString2 = downloadTextContent(url, "UTF-8");
            System.out.println(contentAsString2);
            System.out.println();

            System.out.println("Meta data of object is ...");
            ObjectMetadata objectMetaData = object.getObjectMetadata();

            System.out.format("%-30s: %s%n", "getContentType", object.getObjectMetadata().getContentType());
            System.out.format("%-30s: %s%n", "getContentLength", object.getObjectMetadata().getContentLength());
            System.out.format("%-30s: %s%n", "getContentEncoding", object.getObjectMetadata().getContentEncoding());
            System.out.format("%-30s: %s%n", "getContentDisposition", object.getObjectMetadata().getContentDisposition());
            System.out.format("%-30s: %s%n", "getContentMD5", object.getObjectMetadata().getContentMD5());
            System.out.format("%-30s: %s%n", "getCacheControl", object.getObjectMetadata().getCacheControl());
            System.out.format("%-30s: %s%n", "getETag", object.getObjectMetadata().getETag());
            System.out.format("%-30s: %s%n", "getLastModified", object.getObjectMetadata().getLastModified());
            System.out.format("%-30s: %s%n", "getExpirationTime", object.getObjectMetadata().getExpirationTime());
            System.out.format("%-30s: %s%n", "getHttpExpiresDate", object.getObjectMetadata().getHttpExpiresDate());
            System.out.format("%-30s: %s%n", "getExpirationTimeRuleId", object.getObjectMetadata().getExpirationTimeRuleId());
            System.out.format("%-30s: %s%n", "getInstanceLength", object.getObjectMetadata().getInstanceLength());
            System.out.format("%-30s: %s%n", "getPartCount", object.getObjectMetadata().getPartCount());
            System.out.format("%-30s: %s%n", "getOngoingRestore", object.getObjectMetadata().getOngoingRestore());
            System.out.format("%-30s: %s%n", "getRestoreExpirationTime", object.getObjectMetadata().getRestoreExpirationTime());
            System.out.format("%-30s: %s%n", "getVersionId", object.getObjectMetadata().getVersionId());

            Map<String, String> userMetadata = objectMetaData.getUserMetadata();
            for (String userDataKey : userMetadata.keySet()) {
                System.out.format("USER %-25s: %s%n", userDataKey, userMetadata.get(userDataKey));
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    void createCopyOfExistingObject(String sourceKey, String destinationKey) {
        try {

            String sourceBucketName = bucketName;
            String destinationBucketName = bucketName;
            String sourcePath = prefixWithBucket(sourceBucketName, sourceKey);
            String destinationPath = prefixWithBucket(destinationBucketName, destinationKey);

            printTestCase("Make a copy of an existing object \"" + sourcePath + "\" with a new key \"" + destinationPath + "\"");

            final ObjectMetadata existingObjectMetaData = s3Client.getObjectMetadata(bucketName, sourcePath);

            ObjectMetadata newMetaData = new ObjectMetadata();
            newMetaData.setContentLength(existingObjectMetaData.getContentLength());
            newMetaData.setContentType(existingObjectMetaData.getContentType());
            if (existingObjectMetaData.getUserMetadata() != null) {
                Map<String, String> userMetadata = existingObjectMetaData.getUserMetadata();
                for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
                    System.out.format("USER %-25s: %s%n", entry.getKey(), userMetadata.get(entry.getValue()));
                }
            }

            CopyObjectRequest copyObjectRequest = new CopyObjectRequest(sourceBucketName, sourcePath, destinationBucketName, destinationPath)
                    .withNewObjectMetadata(newMetaData);

            CopyObjectResult result = s3Client.copyObject(copyObjectRequest); // minio: The specified key does not exist.
            System.out.println("Created copy object \"" + destinationPath + "\" with ETag = " + result.getETag());

            System.out.println("Showing the copied object's meta data");

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, destinationPath));
            ObjectMetadata objectMetaData = object.getObjectMetadata();

            System.out.format("%-30s: %s%n", "getETag", object.getObjectMetadata().getETag());
            System.out.format("%-30s: %s%n", "getContentType", object.getObjectMetadata().getContentType());
            System.out.format("%-30s: %s%n", "getContentLength", object.getObjectMetadata().getContentLength());
            System.out.format("%-30s: %s%n", "getContentEncoding", object.getObjectMetadata().getContentEncoding());
            System.out.format("%-30s: %s%n", "getContentMD5", object.getObjectMetadata().getContentMD5());
            System.out.format("%-30s: %s%n", "getLastModified", object.getObjectMetadata().getLastModified());
            System.out.format("%-30s: %s%n", "getVersionId", object.getObjectMetadata().getVersionId());

            Map<String, String> userMetadata = objectMetaData.getUserMetadata();
            for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
                System.out.format("USER %-25s: %s%n", entry.getKey(), userMetadata.get(entry.getValue()));
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    String createNewVersionOfExistingObject(String objectKey) {
        try {
            printTestCase("Create a new version of an existing object - meta data only");

            String objectPath = prefixWithBucket(bucketName, objectKey);

            S3Object existingObject = s3Client.getObject(bucketName, objectPath);
            ObjectMetadata existingObjectMetaData = existingObject.getObjectMetadata();

            S3ObjectInputStream in = existingObject.getObjectContent();

            MessageDigest md = MessageDigest.getInstance("MD5");
            DigestInputStream hashStream = new DigestInputStream(in, md);
            InputStream newIn = buildInMemoryClone(hashStream);
            byte[] digest = md.digest();
            String checkMd5Base64 = Base64.encodeBase64String(digest);
            System.out.println("MD5 of content is (Base64) = " + checkMd5Base64);

            Map<String, String> userMetadata = new HashMap<>(existingObjectMetaData.getUserMetadata());
            userMetadata.put("document-status", "second");

            ObjectMetadata newObjectMetaData = existingObject.getObjectMetadata();
            newObjectMetaData.setUserMetadata(userMetadata);
            newObjectMetaData.setContentLength(existingObjectMetaData.getContentLength());
            newObjectMetaData.setContentType(existingObjectMetaData.getContentType());
            newObjectMetaData.setContentEncoding(existingObjectMetaData.getContentEncoding());

            String md5 = existingObjectMetaData.getContentMD5() == null ? checkMd5Base64 : existingObjectMetaData.getContentMD5();
            newObjectMetaData.setContentMD5(md5);

            PutObjectRequest request = new PutObjectRequest(bucketName, objectPath, newIn, newObjectMetaData);
            PutObjectResult result = s3Client.putObject(request); // not available in minio, unless withChunkedEncodingDisabled(true)
            System.out.println("Created with ETag = " + result.getETag());
            System.out.println("Showing an updated object's meta data");

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectPath));
            ObjectMetadata objectMetaData = object.getObjectMetadata();

            System.out.format("%-30s: %s%n", "getETag", object.getObjectMetadata().getETag());
            System.out.format("%-30s: %s%n", "getContentMD5", object.getObjectMetadata().getContentMD5());
            System.out.format("%-30s: %s%n", "getLastModified", object.getObjectMetadata().getLastModified());
            System.out.format("%-30s: %s%n", "getVersionId", object.getObjectMetadata().getVersionId());

            userMetadata = objectMetaData.getUserMetadata();
            for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
                System.out.format("USER %-25s: %s%n", entry.getKey(), userMetadata.get(entry.getValue()));
            }

            return object.getObjectMetadata().getVersionId();
        } catch (Exception e) {
            handleException(e);
            return null;
        }
    }

    void listVersionsOfExistingObject(String prefix, boolean withMetaData) {
        try {
            printTestCase("List all version ids of an existing object (with meta data = " + withMetaData + ")");

            ListVersionsRequest request = new ListVersionsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withMaxResults(20);

            VersionListing versionListing;
            do {
                versionListing = s3Client.listVersions(request);
                // MINIO: not available in minio, unless withChunkedEncodingDisabled(true)
                // AWS: There is no such thing as the ?versions sub-resource for a key
                for (S3VersionSummary objectSummary : versionListing.getVersionSummaries()) {
                    System.out.print(" - " + objectSummary.getKey() + "  " + "size = " + objectSummary.getSize() + ", versionId = "
                            + objectSummary.getVersionId());
                    if (withMetaData) {
                        S3Object s3Object =
                                s3Client.getObject(new GetObjectRequest(bucketName, objectSummary.getKey(), objectSummary.getVersionId()));
                        String documentStatus = s3Object.getObjectMetadata().getUserMetaDataOf("document-status");
                        System.out.print(", document-status = " + documentStatus);
                    }
                    System.out.println();
                }
                request.setKeyMarker(versionListing.getNextKeyMarker());
                request.setVersionIdMarker(versionListing.getNextVersionIdMarker());
            } while (versionListing.isTruncated());
        } catch (Exception e) {
            handleException(e);
        }
    }

    private int listKeysWithPagingAndNextBatchOf(String prefix, int pageSize, int stopOnMax, boolean printOut) {


        printTestCase("ListKeysWithPagingAndNextBatchOf \"" + prefix + "\"...");
        int count = 0;

        try {
            ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withMaxKeys(pageSize);

            ObjectListing listing = null;

            int loop = 0;

            do {
                if (listing == null) {
                    listing = s3Client.listObjects(request);
                } else {
                    listing = s3Client.listNextBatchOfObjects(listing);
                }

                for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                    if (printOut) {
                        System.out.println(" - " + loop + " " + count + " " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
                    }
                    count++;
                }

                loop++;
            }
            while (listing.isTruncated() && count < stopOnMax);
        } catch (Exception e) {
            handleException(e);
        }

        return count;
    }

    private int listKeysWithPagingAndContinuationToken(String prefix, int pageSize, int stopOnMax, boolean printOut) {


        printTestCase("ListKeysWithPagingAndContinuationToken \"" + prefix + "\"...");

        int count = 0;

        try {
            final ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withMaxKeys(pageSize);
            ListObjectsV2Result listing;

            int loop = 0;
            do {
                listing = s3Client.listObjectsV2(request);

                for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                    if (printOut) {
                        System.out.println(" - " + listing.getNextContinuationToken() + " " + loop + " " + count + " " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
                    }
                    count++;
                }

                request.setContinuationToken(listing.getNextContinuationToken());

                loop++;
            }
            while (listing.isTruncated() && count < stopOnMax);

        } catch (Exception e) {
            handleException(e);
        }

        return count;
    }

    void listAndDeleteTestObjects(String prefix) {
        try {
            printTestCase("Delete the tests objects with prefix \"" + prefix + "\" ...");

            ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withMaxKeys(20);
            ObjectListing objects3 = s3Client.listObjects(request);
            List<DeleteObjectRequest> deletes = new ArrayList<>();
            for (S3ObjectSummary summary : objects3.getObjectSummaries()) {
                deletes.add(new DeleteObjectRequest(bucketName, summary.getKey()));
            }
            for (DeleteObjectRequest deleteObjectRequest : deletes) {
                System.out.println("DEL " + deleteObjectRequest.getBucketName() + "/" + deleteObjectRequest.getKey());
                s3Client.deleteObject(deleteObjectRequest);
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    void deleteTestObjects(String prefix) {

        try {
            printTestCase("Delete the tests objects with prefix \"" + prefix + "\" ...");
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(prefix)
                    .withQuiet(false);

            // Verify that the objects were deleted successfully.
            DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
            int successfulDeletes = delObjRes.getDeletedObjects().size();
            System.out.println(successfulDeletes + " objects successfully deleted.");
        } catch (Exception e) {
            handleException(e);
        }
    }

    // ----------------------------------------------------------------------------------------

    private String prefixWithBucket(String path) {
        return prefixWithBucket(bucketName, path);
    }

    private String prefixWithBucket(String bucketName, String path) {
        if (serviceProperties.isPrefixWithBucketName()) {
            return bucketName + "/" + path;
        } else {
            return path;
        }
    }

    // ----------------------------------------------------------------------------------------

    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("smoke-test-", ".txt");
        file.deleteOnExit();

        try (Writer writer = new OutputStreamWriter(new FileOutputStream(file))) {
            writer.write("abcdefghijklmnopqrstuvwxyz\n");
            writer.write("01234567890112345678901234\n");
            writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
            writer.write("01234567890112345678901234\n");
            writer.write("abcdefghijklmnopqrstuvwxyz\n");
        }

        return file;
    }

    private static void printTestCase(String name) {
        System.out.println();
        System.out.println("------------- " + name + " ------------------------");
    }

    private static InputStream buildInMemoryClone(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int bytesRead;
        while ((bytesRead = in.read(buf)) > 0) {
            out.write(buf, 0, bytesRead);
        }
        return new ByteArrayInputStream(out.toByteArray());
    }

    private static byte[] buildMd5Hash(InputStream in) throws IOException, NoSuchAlgorithmException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buf)) > 0) {
                md.update(buf, 0, bytesRead);
            }
            return md.digest();
        } finally {
            in.close();
        }
    }

    private static String buildMd5HashHexString(InputStream in) throws NoSuchAlgorithmException, IOException {
        return Hex.encodeHexString(buildMd5Hash(in));
    }

    private static String buildMd5HashBase64String(InputStream in) throws NoSuchAlgorithmException, IOException {
        return Base64.encodeBase64String(buildMd5Hash(in));
    }

    private String downloadTextContent(String url, String charSetName) throws Exception {
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);
        InputStream in = response.getEntity().getContent();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        pipeBlobStream(in, out, 4096);
        return new String(out.toByteArray(), Charset.forName(charSetName));
    }

    private String downloadTextContent(InputStream in, String charSetName) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        pipeBlobStream(in, out, 4096);
        return new String(out.toByteArray(), Charset.forName(charSetName));
    }

    private static long pipeBlobStream(InputStream in, OutputStream out, int bufsize)
            throws IOException {
        long size = 0L;
        byte[] buf = new byte[bufsize];
        int bytesRead;
        while ((bytesRead = in.read(buf)) > 0) {
            out.write(buf, 0, bytesRead);
            size += (long) bytesRead;
        }
        return size;
    }

    private static void handleException(Exception e) {
        if (e instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException) e;
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());

            ase.printStackTrace();
        } else if (e instanceof AmazonClientException) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + e.getMessage());

            e.printStackTrace();
        } else {
            e.printStackTrace();
        }
    }
}