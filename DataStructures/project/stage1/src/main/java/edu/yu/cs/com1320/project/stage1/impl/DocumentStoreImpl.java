package edu.yu.cs.com1320.project.stage1.impl;

import edu.yu.cs.com1320.project.stage1.Document;
import edu.yu.cs.com1320.project.stage1.DocumentStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class DocumentStoreImpl implements DocumentStore {
    @Override
    public String setMetadata(URI uri, String key, String value) {
        if (uri == null || uri.isBlank() || key == null || key.isEmpty() || value == null || value.isEmpty()) {
            throw new IllegalArgumentException("null or empty uri, key, or value")
        }
        return "";
    }

    @Override
    public String getMetadata(URI uri, String key) {
        return "";
    }

    @Override
    public int put(InputStream input, URI uri, DocumentFormat format) throws IOException {
        return 0;
    }

    @Override
    public Document get(URI url) {
        return null;
    }

    @Override
    public boolean delete(URI url) {
        return false;
    }
}
