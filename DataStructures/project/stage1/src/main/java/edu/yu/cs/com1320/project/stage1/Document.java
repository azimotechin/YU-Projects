package edu.yu.cs.com1320.project.stage1;

import java.net.URI;
import java.util.HashMap;

public interface Document {

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
}