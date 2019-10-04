package com.giraone.s3.testdocuments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.s3.objectstore.testdata.DynamicConfigGenerator;
import com.itextpdf.text.Document;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.*;

/**
 * A simple PDF document generator.
 */
public class PdfTestDocumentCreator {

    private DynamicConfigGenerator dynamicConfigGenerator;
    private File rootDir = new File(System.getProperty("java.io.tmpdir"));

    public PdfTestDocumentCreator(DynamicConfigGenerator dynamicConfigGenerator) {
        super();
        this.dynamicConfigGenerator = dynamicConfigGenerator;
    }

    public File getRootDir() {
        return rootDir;
    }

    public void setRootDir(File rootDir) {
        this.rootDir = rootDir;
    }

    public FilePair create(int containerIndex, int documentIndex) {
        TestDocumentContent content = this.dynamicConfigGenerator.buildTestDocumentContent(containerIndex, documentIndex);
        File pdfFile, metaDataFile;
        try {
            metaDataFile = File.createTempFile(content.getUuid(), ".json");
            pdfFile = File.createTempFile(content.getUuid(), ".pdf");

            // TODO: we might skip this, when generated documents are used multiple times!
            metaDataFile.deleteOnExit();
            pdfFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        if (this.createPdfFile(pdfFile, content) && this.createMetaDataFile(metaDataFile, content)) {
            return new FilePair(metaDataFile, pdfFile);
        }
        return null;
    }

    private int run(int containerIndex, int documentStartIndex, int numberOfFiles) throws IOException {
        int count = 0;
        for (int documentIndex = documentStartIndex; documentIndex < documentStartIndex + numberOfFiles; documentIndex++) {
            TestDocumentContent content = this.dynamicConfigGenerator.buildTestDocumentContent(containerIndex, documentIndex);
            File pdfFile = new File(rootDir, content.getUuid() + ".pdf");
            File metaDataFile = new File(rootDir, content.getUuid() + ".json");

            if (this.createPdfFile(pdfFile, content) && this.createMetaDataFile(metaDataFile, content)) {
                count++;
            }
        }
        return count;
    }

    private PdfTestDocumentCreator() {
        super();
    }

    public static void main(String[] args) throws Exception {
        PdfTestDocumentCreator creator = new PdfTestDocumentCreator();
        int count = creator.run(0, 100, 0);
        System.out.println(count);
    }

    //---------------------------------------------------------------------------------------------

    private boolean createMetaDataFile(File file, TestDocumentContent content) {
        OutputStream outputStream;
        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        try {
            this.constructJson(content, outputStream);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
            }
        }
        return true;
    }

    private boolean createPdfFile(File file, TestDocumentContent content) {
        OutputStream outputStream;
        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        try {
            this.constructPdf(content, outputStream);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                outputStream.close();
            } catch (IOException ignored) {
            }
        }
        return true;
    }

    private void constructJson(TestDocumentContent content, OutputStream outputStream) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(outputStream, content);
    }

    private void constructPdf(TestDocumentContent content, OutputStream outputStream) throws Exception {
        Document document = new Document();
        PdfWriter.getInstance(document, outputStream);
        try {
            document.open();

            document.addTitle(content.getTitle() + " " + content.getUuid());
            document.add(new Paragraph(content.getTitle()));
            document.add(new Paragraph("UUID = " + content.getUuid()));
            document.add(new Paragraph("Counter = " + content.getCounter()));
            document.add(new Paragraph("Time Millis = " + content.getTime()));
            document.add(new Paragraph("Date = " + content.getDate()));

            for (String key : content.getMetaData().keySet()) {
                document.add(new Paragraph(key + " = " + content.getMetaData().get(key)));
            }
            document.add(new Paragraph("-----------------------------------------------------------------------"));

            document.add(new Paragraph(content.getText()));
        } finally {
            document.close();
        }
    }

    public class FilePair {
        public File jsonFile;
        public File pdfFile;

        FilePair(File jsonFile, File pdfFile) {
            this.jsonFile = jsonFile;
            this.pdfFile = pdfFile;
        }
    }
}
