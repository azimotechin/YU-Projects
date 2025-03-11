package edu.yu.cs.com1320.project.stage4.impl;

import edu.yu.cs.com1320.project.HashTable;
import edu.yu.cs.com1320.project.impl.HashTableImpl;
import edu.yu.cs.com1320.project.stage4.Document;

import java.net.URI;
import java.util.Arrays;
import java.util.Set;

public class DocumentImpl implements Document {
    // class variables
    private final HashTable<String, String> metadata;
    private String text;
    private byte[] binaryData;
    private final URI uri;

    // constructors
    public DocumentImpl (URI uri, String txt) {
        this.metadata = new HashTableImpl<>();
        this.uri = uri;
        this.text = txt;
    }
    public DocumentImpl (URI uri, byte[] binaryData) {
        this.metadata = new HashTableImpl<>();
        this.uri = uri;
        this.binaryData = binaryData;
    }

    // getters and setters
    @Override
    public String setMetadataValue(String key, String value) {
        String oldValue = this.getMetadataValue(key);
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
    public HashTable<String, String> getMetadata() {
        HashTable<String, String> copy = new HashTableImpl<>();
        for (String key : this.metadata.keySet()) {
            copy.put(key, this.metadata.get(key));
        }
        return copy;
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

    @Override
    public int wordCount(String word) {
        return 0;
    }

    @Override
    public Set<String> getWords() {
        return Set.of();
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
