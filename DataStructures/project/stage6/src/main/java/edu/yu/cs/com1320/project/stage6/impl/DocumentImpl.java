package edu.yu.cs.com1320.project.stage6.impl;

import edu.yu.cs.com1320.project.stage6.Document;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DocumentImpl implements Document, Comparable<Document>{
    // class variables
    private HashMap<String, String> metadata;
    private Map<String, Integer> wordCountMap;
    private String text;
    private byte[] binaryData;
    private final URI uri;
    private long lastUseTime;

    // constructors
    public DocumentImpl (URI uri, String text, Map<String, Integer> wordCountMap) {
        this.metadata = new HashMap<>();
        this.uri = uri;
        this.text = text;
        this.wordCountMap = wordCountMap;
        this.lastUseTime = System.nanoTime();
    }
    public DocumentImpl (URI uri, byte[] binaryData) {
        this.wordCountMap = null;
        this.metadata = new HashMap<>();
        this.uri = uri;
        this.binaryData = binaryData;
        this.lastUseTime = -1;
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
    public HashMap<String, String> getMetadata() {
        if (metadata == null) {
            return new HashMap<>();
        }
        return new HashMap<>(metadata);
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
        if (metadata == null) {
            this.metadata = new HashMap<>();
            return;
        }
        this.metadata = new HashMap<>(metadata);
    }

    @Override
    public String getDocumentTxt() {
        return this.text;
    }

    @Override
    public byte[] getDocumentBinaryData() {
        return this.binaryData;
    }

    @Override
    public URI getKey() {
        return this.uri;
    }

    //words
    @Override
    public int wordCount(String word) {
        return this.wordCountMap == null ? 0 : this.wordCountMap.getOrDefault(word, 0);
    }

    @Override
    public Set<String> getWords() {
        return this.wordCountMap == null ? Set.of() : this.wordCountMap.keySet();
    }

    @Override
    public long getLastUseTime() {
        return this.lastUseTime;
    }

    @Override
    public void setLastUseTime(long timeInNanoseconds) {
        this.lastUseTime = timeInNanoseconds;
    }

    @Override
    public HashMap<String, Integer> getWordMap() {
        return wordCountMap == null ? new HashMap<>() : new HashMap<>(wordCountMap);
    }

    @Override
    public void setWordMap(HashMap<String, Integer> wordMap) {
        this.wordCountMap = wordMap == null ? null : new HashMap<>(wordMap);
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

    @Override
    public int compareTo(Document o) {
        return Long.compare(this.getLastUseTime(), o.getLastUseTime());
    }
}
