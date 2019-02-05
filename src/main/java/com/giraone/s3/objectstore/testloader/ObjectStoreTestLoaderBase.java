package com.giraone.s3.objectstore.testloader;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.s3.objectstore.config.ObjectStorageEnvironment;
import com.giraone.s3.objectstore.testdata.DefaultDynamicConfigGenerator;
import com.giraone.s3.objectstore.testdata.DocumentMetaData;
import com.giraone.s3.objectstore.testdata.DynamicConfigGenerator;
import com.giraone.s3.objectstore.testdata.TestConfig;
import com.giraone.s3.testdocuments.PdfTestDocumentCreator;
import org.apache.commons.cli.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public abstract class ObjectStoreTestLoaderBase {
    ObjectStorageEnvironment env;

    private ExecutorService executor;

    private PdfTestDocumentCreator pdfCreator;
    protected TestConfig testConfig;
    protected ResultInfo resultInfo;

    private final MetricRegistry metrics = new MetricRegistry();

    protected final Timer monitorPdfDocumentCreation = metrics.timer(MetricRegistry.name(ObjectStoreTestLoaderBase.class, "PdfDocumentCreation"));
    protected final Timer monitorContainerCreation = metrics.timer(MetricRegistry.name(ObjectStoreTestLoaderBase.class, "ContainerCreation"));
    protected final Timer monitorDocumentUpload = metrics.timer(MetricRegistry.name(ObjectStoreTestLoaderBase.class, "DocumentUpload"));

    ObjectStoreTestLoaderBase() {
        super();
    }

    abstract boolean createContainer(int containerIndex);

    abstract String createDocument(String rootContainerName, String objectPath, File jsonFile, File pdfFile);

    abstract boolean checkRootContainer(String name);

    private void setTestConfig(TestConfig testConfig) {
        this.testConfig = testConfig;
        this.resultInfo = new ResultInfo(
                testConfig.getNumberOfContainers(),
                testConfig.getNumberOfDocumentsPerContainer());
        this.pdfCreator = new PdfTestDocumentCreator(testConfig.getDynamicConfigGenerator());
    }

    private void setEnv(ObjectStorageEnvironment env) {
        this.env = env;
    }

    protected Map<String, String> buildObjectMetaData(DocumentMetaData metaData) {
        Map<String, String> objectMetaDataMap = new HashMap<>();
        objectMetaDataMap.put("title", metaData.getTitle());
        objectMetaDataMap.put("uuid", metaData.getUuid());
        objectMetaDataMap.put("date", new Date(metaData.getTime()).toString());
        metaData.getMetaData().forEach((key,value) -> objectMetaDataMap.put(key, value));
        return objectMetaDataMap;
    }

    private boolean createDocument(String rootContainerName, int containerIndex, int documentIndex) {

        String containerName = testConfig.getDynamicConfigGenerator().buildContainerName(containerIndex);
        String objectPath = testConfig.getDynamicConfigGenerator().buildPathNames(containerIndex, documentIndex);

        final Timer.Context context = monitorPdfDocumentCreation.time();
        PdfTestDocumentCreator.FilePair filePair = pdfCreator.create(containerIndex, documentIndex);
        context.stop();

        File jsonFile = filePair.jsonFile;
        File pdfFile = filePair.pdfFile;

        String etag = this.createDocument(rootContainerName, containerName + "/" + objectPath, jsonFile, pdfFile);
        if (etag != null) {
            resultInfo.addDocumentResultOk();
        } else {
            resultInfo.addDocumentResultError(1, "failed");
            return false;
        }

        return true;
    }

    /**
     * Create a certain number of documents within a given container or within newly created containers
     *
     * @param numberOfContainers   A number of containers or 0, when a single default container is used.
     * @param defaultContainerName The root container name, when numberOfContainers is 0.
     * @return the total number of successfully created documents
     */
    private int createDocuments(int numberOfContainers, String defaultContainerName) {
        printState("fillContainers START");

        long start = System.currentTimeMillis();

        // Create a list to hold the Future objects associated with Callable
        List<Future<Integer>> resultList = new ArrayList<Future<Integer>>();

        for (int containerIndex = 0; containerIndex < (numberOfContainers == 0 ? 1 : numberOfContainers); containerIndex++) {
            final int finalContainerIndex = containerIndex;
            final int[] documentIndex = new int[]{0};

            Callable<Integer> callable = new Callable<Integer>() {

                @Override
                public Integer call() throws Exception {
                    printState(" fillContainer START " + finalContainerIndex);
                    int count = 0;
                    while (documentIndex[0] < testConfig.getNumberOfDocumentsPerContainer()) {
                        documentIndex[0] = ++documentIndex[0];
                        if (!createDocument(defaultContainerName, numberOfContainers == 0 ? -1 : finalContainerIndex, documentIndex[0])) {
                            break;
                        }
                        count++;
                    }
                    printState(" fillContainer END  " + finalContainerIndex);
                    return count;
                }
            };

            // Submit Callable tasks to be executed by thread pool
            Future<Integer> future = executor.submit(callable);
            // Add Future to the list, so we can get return value using Future
            resultList.add(future);
        }

        int totalCount = 0;
        for (Future<Integer> future : resultList) {
            // because Future.get() waits for task to get completed, we can sum up the total number of created documents
            try {
                totalCount += future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // Shut down the executor service now
        executor.shutdown();

        long end = System.currentTimeMillis();

        printState("fillContainers END");

        System.out.println("* Total duration = " + (end - start) + " msecs.");

        printMonitorData(monitorPdfDocumentCreation, "PdfDocumentCreation");
        printMonitorData(monitorDocumentUpload, "DocumentUpload");

        return totalCount;
    }

    private int createContainers() {
        printState("createContainers START");

        int ret = 0;
        for (int containerIndex = 0; containerIndex < this.testConfig.getNumberOfContainers(); containerIndex++) {
            if (!this.createContainer(containerIndex)) {
                break;
            }
            ret++;
        }

        printState("createContainers END");

        printMonitorData(monitorContainerCreation, "ContainerCreation");

        return ret;
    }

    protected void run(TestConfig testConfig, ObjectStorageEnvironment env) {
        this.setTestConfig(testConfig);

        this.setEnv(env);

        // Get ExecutorService from Executors utility class. The thread pool size is derived from the configuration
        this.executor = Executors.newFixedThreadPool(testConfig.getNumberOfThreads());

        printState("RUNNING with " + testConfig.getNumberOfThreads() + " threads");

        // Create no containers, but only documents in a certain container
        if (!this.checkRootContainer(testConfig.getRootContainerName())) {
            throw new IllegalArgumentException("No root container \"" + testConfig.getRootContainerName() + "\"!");
        }

        // Create n container and then m documents
        int numberOfContainers = this.createContainers();
        System.out.println("Number of created containers: " + numberOfContainers);
        int numberOfDocuments = this.createDocuments(numberOfContainers, testConfig.getRootContainerName());
        System.out.println("Number of created documents: " + numberOfDocuments);
    }

    // ---------------------------------------------------------------------------------

    protected DocumentMetaData parseFile(File jsonFile) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonFile, DocumentMetaData.class);
    }

    // ---------------------------------------------------------------------------------

    protected static TestConfig parseCli(String[] args) {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();

        options.addOption("h", "help", false, "print usage help");

        Option numberOfThreadsOption = Option.builder()
                .longOpt("threads")
                .hasArg()
                .desc("number of parallel threads, default is 1")
                .build();
        options.addOption(numberOfThreadsOption);

        Option numberOfContainersOption = Option.builder()
                .longOpt("containers")
                .hasArg()
                .desc("number of containers, default is 0 - then root must be present")
                .build();
        options.addOption(numberOfContainersOption);

        Option bucketOption = Option.builder()
                .longOpt("bucket")
                .hasArg()
                .desc("name of bucket, no default")
                .build();
        options.addOption(bucketOption);

        Option rootContainerOption = Option.builder()
                .longOpt("root")
                .hasArg()
                .desc("name of existing root container to be used, if empty the new containers are top-level")
                .build();
        options.addOption(rootContainerOption);

        Option numberOfDocumentsOption = Option.builder()
                .longOpt("docs")
                .hasArg()
                .desc("number of documents per container, default is 10")
                .build();
        options.addOption(numberOfDocumentsOption);

        Option configClassOption = Option.builder()
                .longOpt("config")
                .hasArg()
                .desc("dynamic configurator class, default is com.giraone.s3.objectstore.testdata.DefaultDynamicConfigGenerator")
                .build();
        options.addOption(configClassOption);

        TestConfig testConfig = new TestConfig();

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                usage(options);
                System.exit(1);
            }

            String configClassName = line.getOptionValue("config", DefaultDynamicConfigGenerator.class.getCanonicalName());
            DynamicConfigGenerator dynamicConfigGenerator = (DynamicConfigGenerator) Class.forName(configClassName).newInstance();
            testConfig.setDynamicConfigGenerator(dynamicConfigGenerator);
            testConfig.setBucketName(line.getOptionValue("bucket"));
            testConfig.setRootContainerName(line.getOptionValue("root", ""));
            testConfig.setNumberOfContainers(Integer.parseInt(line.getOptionValue("containers", "0")));
            testConfig.setNumberOfDocumentsPerContainer(Integer.parseInt(line.getOptionValue("docs", "10")));
            testConfig.setNumberOfThreads(Integer.parseInt(line.getOptionValue("threads", "1")));
        } catch (Exception exp) {
            exp.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ObjectStoreTestLoader", options);
            return null;
        }

        return testConfig;
    }

    // ---------------------------------------------------------------------------------

    private static void usage(Options options) {

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar target/testdata-loader-1.0.jar\n", options);
    }

    protected static void printMonitorData(Timer timer, String name) {
        System.out.println("* " + name);
        System.out.println("* Count   = " + timer.getCount());
        System.out.println("* Average = " + timer.getMeanRate() + " msecs");
    }

    protected static void printState(String name) {
        System.out.print("------- ");
        System.out.print(name);
        System.out.println(" ---------");
    }
}
