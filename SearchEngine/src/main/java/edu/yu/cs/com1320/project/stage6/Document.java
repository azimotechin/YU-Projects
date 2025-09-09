package edu.yu.cs.com1320.project.stage6;

import java.net.URI;
import java.util.HashMap;
import java.util.Set;

public interface Document extends Comparable<Document> {
    /**
     * @param key   key of document metadata to store a value for
     * @param value value to store
     * @return old value, or null if there was no old value
     * @throws IllegalArgumentException if the key is null or blank
     */
    String setMetadataValue(String key, String value);

    /**
     * @param key metadata key whose value we want to retrieve
     * @return corresponding value, or null if there is no such key
     * @throws IllegalArgumentException if the key is null or blank
     */
    String getMetadataValue(String key);

    /**
     * @return a COPY of the metadata saved in this document
     */
    HashMap<String, String> getMetadata();

    void setMetadata(HashMap<String, String> metadata);

    /**
     * @return content of text document
     */
    String getDocumentTxt();

    /**
     * @return content of binary data document
     */
    byte[] getDocumentBinaryData();

    /**
     * @return URI which uniquely identifies this document
     */
    URI getKey();

    /**
     * how many times does the given word appear in the document?
     *
     * @param word
     * @return the number of times the given words appears in the document. If it's a binary document, return 0.
     */
    int wordCount(String word);

    /**
     * @return all the words that appear in the document
     */
    Set<String> getWords();


    /**
     * return the last time this document was used, via put/get or via a search result
     * (for stage 4 of project)
     */
    long getLastUseTime();

    void setLastUseTime(long timeInNanoseconds);

    /**
     * @return a copy of the word to count map so it can be serialized
     */
    HashMap<String, Integer> getWordMap();

    /**
     * This must set the word to count map during deserialization
     *
     * @param wordMap
     */
    void setWordMap(HashMap<String, Integer> wordMap);
}