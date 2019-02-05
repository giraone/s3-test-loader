package com.giraone.s3.objectstore.testdata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import com.giraone.s3.testdocuments.TestDocumentContent;

public class DefaultDynamicConfigGenerator implements DynamicConfigGenerator {
    static int MilliSecondsOfAYear = 1000 * 3600 * 24 * 365;

    Random random = new Random();

    long startDate = System.currentTimeMillis() - MilliSecondsOfAYear * 2;
    String dateFormat = "yyyy-MM-dd";

    public static final String[] TYPE_NAMES = new String[]{
            "Rechnungsbeleg", "Spendenquittung", "Sonstiger Beleg"
    };

    public static final String[] PATH_NAMES = new String[]{
            "Steuer 2014/Handwerkerrechnungen", "Steuer 2014/Arbeitsmittel",
            "Steuer 2014/Spendenquittungen", "Steuer 2014/Sonderausgaben", "Steuer 2014/Sonstige Einkünfte",
            "Steuer 2015/Handwerkerrechnungen", "Steuer 2015/Arbeitsmittel",
            "Steuer 2015/Spendenquittungen", "Steuer 2015/Sonderausgaben", "Steuer 2015/Sonstige Einkünfte"
    };

    public String buildContainerName(int containerIndex) {
        return String.format("Container-%07d", containerIndex);
    }

    public String buildPathNames(int containerIndex, int documentIndex) {
        return PATH_NAMES[documentIndex % PATH_NAMES.length];
    }

    public TestDocumentContent buildTestDocumentContent(int containerIndex, int documentIndex) {
        long time = startDate + random.nextInt(MilliSecondsOfAYear);
        Date date = new Date(time);
        String dateString = new SimpleDateFormat(dateFormat).format(date);
        int taxYear = random.nextInt(1) + 2014;
        String type = TYPE_NAMES[documentIndex % TYPE_NAMES.length];
        String title = type + "-" + dateString + "-" + String.format("%06d", documentIndex) + ".pdf";

        HashMap<String, String> metaData = new HashMap<String, String>();
        metaData.put("title", title);
        metaData.put("status", "ADDED");
        metaData.put("taxYear", Integer.toString(taxYear));
        metaData.put("type", type);
        metaData.put("dateString", dateString);

        return new TestDocumentContent(documentIndex, title, time, metaData);
    }
}
