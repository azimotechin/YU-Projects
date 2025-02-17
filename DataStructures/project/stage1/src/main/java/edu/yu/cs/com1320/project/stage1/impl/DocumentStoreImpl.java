package edu.yu.cs.com1320.project.stage1.impl;

import edu.yu.cs.com1320.project.stage1.Document;
import edu.yu.cs.com1320.project.stage1.DocumentStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DocumentStoreImpl implements DocumentStore {
    private final Map<URI, Document> documents;
    public DocumentStoreImpl () {
        this.documents = new HashMap<>();
    }
    @Override
    public String setMetadata(URI uri, String key, String value) {
        getMetadata(uri, key);
        return get(uri).setMetadataValue(key, value);
    }

    @Override
    public String getMetadata(URI uri, String key) {
        if (uri == null || uri.toString().isEmpty() || get(uri) == null) {
            throw new IllegalArgumentException("uri is null, blank, or has no document stored by it");
        }
        return get(uri).getMetadataValue(key);
    }

    @Override
    public int put(InputStream input, URI uri, DocumentFormat format) throws IOException {
        if (uri == null || uri.toString().isEmpty() || format == null) {
            throw new IllegalArgumentException("uri is null or empty or format is null");
        }
        int prev_doc_hash_code = get(uri) != null ? get(uri).hashCode() : 0;
        if (input == null) {
            delete(uri);
            return prev_doc_hash_code;
        }
        Document document = null;
        if (format == DocumentFormat.TXT) {
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            document = new DocumentImpl(uri, text);
        }
        if (format == DocumentFormat.BINARY) {
            byte[] binaryData = input.readAllBytes();
            document = new DocumentImpl(uri, binaryData);
        }
        if (document != null) {
            this.documents.put(uri, document);
        }
        return prev_doc_hash_code;
    }

    @Override
    public Document get(URI url) {
        return this.documents.get(url);
    }

    @Override
    public boolean delete(URI url) {
       if (get(url) == null) {
           return false;
       }
       this.documents.remove(url);
       return true;
    }
}
