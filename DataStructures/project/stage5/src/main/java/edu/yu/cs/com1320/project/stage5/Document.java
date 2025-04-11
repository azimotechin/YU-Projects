package edu.yu.cs.com1320.project.stage5;

import edu.yu.cs.com1320.project.HashTable;

import java.net.URI;
import java.util.Set;

public interface Document extends Comparable<Document>
{
    /**
     * @param key key of document metadata to store a value for
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
    HashTable<String, String> getMetadata();
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
}