package com.giraone.s3.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Scanner;

/**
 * Utilities for reading JSON or text files from the class path.
 */
public class TextFileReader {

    private static final Log log = LogFactory.getLog(TextFileReader.class);

    // Hide instance
    private TextFileReader() {

    }

    /**
     * Read a text file from the resources folder (class path).
     *
     * @param resourcePath relative path, e.g. "config.txt"
     * @return The content of the resource file as a string.
     */
    public static String readTextFileFromResource(String resourcePath) {
        ClassLoader classLoader = TextFileReader.class.getClassLoader();
        URL url = classLoader.getResource(resourcePath);
        if (url == null) {
            throw new IllegalArgumentException("Resource \"" + resourcePath + "\" not found!");
        }
        File file = new File(url.getFile());
        StringBuilder result = new StringBuilder();
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                result.append(line).append("\n");
            }
        } catch (IOException ioe) {
            log.error("Cannot read resource file " + resourcePath, ioe);
        }
        return result.toString();
    }

    /**
     * Read a JSON file from the resources folder (class path).
     *
     * @param resourcePath relative path, e.g. "config.json"
     * @param valueType    the to which the content is deserialized.
     * @return The parsed object
     */
    public static Object readJsonFileFromResource(String resourcePath, Class<?> valueType) throws IOException {
        ClassLoader classLoader = TextFileReader.class.getClassLoader();
        URL url = classLoader.getResource(resourcePath);
        if (url != null) {
            try (InputStream in = url.openStream()) {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(in, valueType);
            }
        } else {
            throw new IllegalStateException("No \"" + resourcePath + "\" file in class path!");
        }
    }

    /**
     * Read a JSON file from a file path
     *
     * @param file      the file to read
     * @param valueType the to which the content is deserialized.
     * @return The parsed object
     */
    public static Object readJsonFileFromPath(File file, Class<?> valueType) {

        ObjectMapper mapper = new ObjectMapper();
        try (FileInputStream in = new FileInputStream(file)) {
            return mapper.readValue(in, valueType);
        } catch (IOException ioe) {
            log.error("Cannot read JSON file " + file, ioe);
            return null;
        }
    }
}
