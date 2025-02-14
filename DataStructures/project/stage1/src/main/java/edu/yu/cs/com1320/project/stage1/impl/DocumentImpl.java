package edu.yu.cs.com1320.project.stage1.impl;

import edu.yu.cs.com1320.project.stage1.Document;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DocumentImpl implements Document {
    //class variables
    private final Map<String, String> metadata;
    private String text;
    private byte[] binaryData;
    private final URI uri;
    //constructors
    public DocumentImpl (URI uri, String txt) {
        this.metadata = new HashMap<>();
        this.uri = uri;
        this.text = txt;
    }
    public DocumentImpl (URI uri, byte[] binaryData) {
        this.metadata = new HashMap<>();
        this.uri = uri;
        this.binaryData = binaryData;
    }
    //getters and setters
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
        return new HashMap<>(this.metadata);
    }
    @Override
    public String getDocumentTxt() {
        try{
            return this.text;
        } catch (NullPointerException e){
            return null;
        }
    }
    @Override
    public byte[] getDocumentBinaryData() {
        try{
            return this.binaryData;
        } catch (NullPointerException e) {
            return null;
        }
    }
    @Override
    public URI getKey() {
        return this.uri;
    }
    //comparators
    @Override
    public int hashCode() {
        int result = uri.hashCode();
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(binaryData);
        return Math.abs(result);
    }
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return this.hashCode() == other.hashCode();
    }
}
