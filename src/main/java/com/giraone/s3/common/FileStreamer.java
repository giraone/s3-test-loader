package com.giraone.s3.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.*;

/**
 * A class to stream byte content from a file.
 */
public class FileStreamer {
    private final static Marker LOG_TAG = MarkerManager.getMarker("CONTROL");
    private final static Logger LOGGER = LogManager.getLogger(FileStreamer.class);

    private static final int BUFSIZE = 4096;

    // Class has only static methods
    private FileStreamer() {
    }

    /**
     * Stream the content of a given file to a given output stream.
     *
     * @param file The file to be streamed
     * @param out  The output stream into which the content is streamed. The stream will not be closed.
     * @return The number of transferred bytes.
     * @throws IOException
     */
    public static long streamFileContent(File file, OutputStream out) {
        long ret = -1L;
        FileInputStream in;
        try {
            in = new FileInputStream(file);
        } catch (IOException ioe) {
            LOGGER.warn(LOG_TAG, "cannot open source file", ioe);
            return ret;
        }

        try {
            ret = pipeBlobStream(in, out, BUFSIZE);
        } catch (IOException ioe) {
            LOGGER.warn(LOG_TAG, "error while streaming", ioe);
            return ret;
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (Exception ignore) {
                }
        }
        return ret;
    }

    /**
     * Stream the content of a given input stream into a file.
     *
     * @param in   The input stream from which the content is read. The stream will not be closed!
     * @param file The file that will be created and overwritten, if it exists!
     * @return The number of transferred bytes.
     * @throws IOException
     */
    public static long streamFileContent(InputStream in, File file) {
        long ret = -1L;
        FileOutputStream out;
        try {
            out = new FileOutputStream(file);
        } catch (IOException ioe) {
            LOGGER.warn(LOG_TAG, "cannot open target file", ioe);
            return ret;
        }

        try {
            ret = pipeBlobStream(in, out, BUFSIZE);
        } catch (IOException ioe) {
            LOGGER.warn(LOG_TAG, "error while streaming", ioe);
            return ret;
        } finally {
            if (in != null)
                try {
                    out.close();
                } catch (Exception ignore) {
                }
        }
        return ret;
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
}