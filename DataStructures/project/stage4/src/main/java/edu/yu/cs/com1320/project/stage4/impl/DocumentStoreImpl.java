package edu.yu.cs.com1320.project.stage4.impl;

import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.stage4.*;
import edu.yu.cs.com1320.project.undo.Command;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class DocumentStoreImpl implements DocumentStore {
    // variables
    private final HashTable<URI, Document> documents;
    private final Stack<Command> commandStack;

    // constructor
    public DocumentStoreImpl () {
        this.documents = new HashTableImpl<>();
        this.commandStack = new StackImpl<>();
    }

    // set metadata
    @Override
    public String setMetadata(URI uri, String key, String value) {
        this.getMetadata(uri, key);
        String str = this.getMetadata(uri, key);
        Consumer<URI> consumer = url -> this.get(url).setMetadataValue(key, str);
        Command command = new Command(uri, consumer);
        this.commandStack.push(command);
        return get(uri).setMetadataValue(key, value);
    }

    // get metadata
    @Override
    public String getMetadata(URI uri, String key) {
        if (uri == null || uri.toString().isEmpty() || get(uri) == null) {
            throw new IllegalArgumentException("uri is null, blank, or has no document stored by it");
        }
        return get(uri).getMetadataValue(key);
    }

    // doc setter (and deleter)
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
        Document doc = get(uri);
        Consumer<URI> consumer = url -> this.documents.put(url, doc);
        Command command = new Command(uri, consumer);
        this.commandStack.push(command);
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

    // get doc
    @Override
    public Document get(URI url) {
        return this.documents.get(url);
    }

    // deleter
    @Override
    public boolean delete(URI url) {
       if (get(url) == null) {
           return false;
       }
       Document doc = get(url);
       Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
       Command command = new Command(url, consumer);
       this.commandStack.push(command);
       this.documents.put(url, null);
       return true;
    }

    // undo last action
    @Override
    public void undo() throws IllegalStateException {
        try {
            this.commandStack.pop().undo();
        } catch (NullPointerException e) {
            throw new IllegalStateException("command stack is empty");
        }
    }

    // undo last action on specific doc
    @Override
    public void undo(URI url) throws IllegalStateException {
        Stack<Command> temp = new StackImpl<>();
        while (this.commandStack.peek() != null && this.commandStack.peek().getUri() != url) {
            temp.push(commandStack.pop());
        }
        try {
            this.commandStack.pop().undo();
        } catch (NullPointerException e) {
            throw new IllegalStateException("command stack is empty");
        }
        while (temp.peek() != null) {
            this.commandStack.push(temp.pop());
        }
    }

    @Override
    public List<Document> search(String keyword) {
        return List.of();
    }

    @Override
    public List<Document> searchByPrefix(String keywordPrefix) {
        return List.of();
    }

    @Override
    public Set<URI> deleteAll(String keyword) {
        return Set.of();
    }

    @Override
    public Set<URI> deleteAllWithPrefix(String keywordPrefix) {
        return Set.of();
    }

    @Override
    public List<Document> searchByMetadata(Map<String, String> keysValues) {
        return List.of();
    }

    @Override
    public List<Document> searchByKeywordAndMetadata(String keyword, Map<String, String> keysValues) {
        return List.of();
    }

    @Override
    public List<Document> searchByPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        return List.of();
    }

    @Override
    public Set<URI> deleteAllWithMetadata(Map<String, String> keysValues) {
        return Set.of();
    }

    @Override
    public Set<URI> deleteAllWithKeywordAndMetadata(String keyword, Map<String, String> keysValues) {
        return Set.of();
    }

    @Override
    public Set<URI> deleteAllWithPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        return Set.of();
    }
}
