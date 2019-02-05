package com.giraone.s3.objectstore.testdata;

import com.giraone.s3.testdocuments.TestDocumentContent;

public interface DynamicConfigGenerator {
    public String buildContainerName(int containerIndex);

    public String buildPathNames(int containerIndex, int documentIndex);

    public TestDocumentContent buildTestDocumentContent(int containerIndex, int documentIndex);
}