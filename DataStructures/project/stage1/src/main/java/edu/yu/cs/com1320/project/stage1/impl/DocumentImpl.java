package edu.yu.cs.com1320.project.stage1.impl;

import edu.yu.cs.com1320.project.stage1.Document;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DocumentImpl implements Document {
    private final Map<String, String> metadata = new HashMap<>();
    public DocumentImpl (Map<String, String> metadata) {
        this.metadata = metadata;
    }
    @Override
    public String setMetadataValue(String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        String oldValue = this.metadata.get(key);
        this.metadata.put(key,value);
        return oldValue;
    }

    @Override
    public String getMetadataValue(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        if (this.metadata.containsKey(key)) {
            return this.metadata.get(key);
        }
        return null;
    }

    @Override
    public HashMap<String, String> getMetadata() {
        Map<String, String> map = new HashMap<>();
        map.putAll(this.metadata);
        return map;
    }

    @Override
    public String getDocumentTxt() {
        return "";
    }

    @Override
    public byte[] getDocumentBinaryData() {
        return new byte[0];
    }

    @Override
    public URI getKey() {
        return null;
    }
    @Override
    public int hashCode() {
        int result = uri.hashCode();
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(binaryData);
        return Math.abs(result);
    }
    @Override
    public boolean equals(URI other) {
        if (this.hashCode() == other.hashCode()) {
            return true;
        }
        return false;
    }
}
