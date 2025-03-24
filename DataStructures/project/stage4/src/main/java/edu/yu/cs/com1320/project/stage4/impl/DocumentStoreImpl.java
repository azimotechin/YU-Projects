package edu.yu.cs.com1320.project.stage4.impl;

import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.Stack;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.stage4.*;
import edu.yu.cs.com1320.project.undo.CommandSet;
import edu.yu.cs.com1320.project.undo.GenericCommand;
import edu.yu.cs.com1320.project.undo.Undoable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

public class DocumentStoreImpl implements DocumentStore {
    // variables
    private final HashTable<URI, Document> documents;
    private final Stack<Undoable> commandStack;
    private final Trie<Document> docTrie;
    // constructor
    public DocumentStoreImpl () {
        this.documents = new HashTableImpl<>();
        this.commandStack = new StackImpl<>();
        this.docTrie = new TrieImpl<>();
    }

    // set metadata
    @Override
    public String setMetadata(URI uri, String key, String value) {
        this.getMetadata(uri, key);
        String str = this.getMetadata(uri, key);
        Consumer<URI> consumer = url -> this.get(url).setMetadataValue(key, str);
        Undoable command = new GenericCommand<>(uri, consumer);
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
        Undoable command = new GenericCommand<>(uri, consumer);
        this.commandStack.push(command);
        Document document = docCreate(input, uri, format);
        if (document != null) {
            this.documents.put(uri, document);
            for (String s : document.getWords()) {
                this.docTrie.put(s,document);
            }
        }
        return prev_doc_hash_code;
    }

    private Document docCreate(InputStream input, URI uri, DocumentFormat format) throws IOException {
        Document document = null;
        if (format == DocumentFormat.TXT) {
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            document = new DocumentImpl(uri, text);
        }
        if (format == DocumentFormat.BINARY) {
            byte[] binaryData = input.readAllBytes();
            document = new DocumentImpl(uri, binaryData);
        }
        return document;
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
       Undoable command = new GenericCommand<>(url, consumer);
       this.commandStack.push(command);
       this.documents.put(url, null);
        for (String s : doc.getWords()) {
            this.docTrie.delete(s,doc);
        }       return true;
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
        Stack<Undoable> temp = new StackImpl<>();
        while (this.commandStack.peek() != null) {
            Undoable command = this.commandStack.peek();
            if (command instanceof CommandSet) {
                @SuppressWarnings("unchecked")
                CommandSet<URI> commandSet = (CommandSet<URI>) command;
                if (commandSet.containsTarget(url)) {
                    break;
                }
            }
            if (command instanceof GenericCommand) {
                @SuppressWarnings("unchecked")
                GenericCommand<URI> genericCommand = (GenericCommand<URI>) command;
                if (genericCommand.getTarget() == url) {
                    break;
                }
            }
            temp.push(this.commandStack.pop());
        }
        if (this.commandStack.peek() instanceof CommandSet) {
            @SuppressWarnings("unchecked")
            CommandSet<URI> commandSet = (CommandSet<URI>) this.commandStack.peek();
            if (commandSet.undo(url)) {
                this.commandStack.pop();
            }
        } else try {
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
        Comparator<Document> comp = Comparator.comparingInt((Document doc) -> doc.wordCount(keyword)).reversed();
        return this.docTrie.getSorted(keyword, comp);
    }

    @Override
    public List<Document> searchByPrefix(String keywordPrefix) {
        Comparator<Document> comp = Comparator.comparingInt((Document doc) -> doc.wordCount(keywordPrefix)).reversed();
        return this.docTrie.getAllWithPrefixSorted(keywordPrefix, comp);
    }

    @Override
    public Set<URI> deleteAll(String keyword) {
        CommandSet<URI> command = new CommandSet<>();
        Set<Document> hits = this.docTrie.deleteAll(keyword);
        for (Document doc : hits) {
            Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
            GenericCommand<URI> genericCommand = new GenericCommand<>(doc.getKey(), consumer);
            if (hits.size() == 1) {
                this.commandStack.push(genericCommand);

            }
            command.addCommand(genericCommand);
        }
        if (hits.size() > 1) {
            this.commandStack.push(command);
        }
        return deletionSet(hits);
    }

    @Override
    public Set<URI> deleteAllWithPrefix(String keywordPrefix) {
        CommandSet<URI> command = new CommandSet<>();
        Set<Document> hits = this.docTrie.deleteAllWithPrefix(keywordPrefix);
        for (Document doc : hits) {
            Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
            GenericCommand<URI> genericCommand = new GenericCommand<>(doc.getKey(), consumer);
            if (hits.size() == 1) {
                this.commandStack.push(genericCommand);
            }
            command.addCommand(genericCommand);
        }
        if (hits.size() > 1) {
            this.commandStack.push(command);
        }
        return deletionSet(hits);
    }

    @Override
    public List<Document> searchByMetadata(Map<String, String> keysValues) {
        List<Document> list = new ArrayList<>();
        for (Document d : this.documents.values()) {
            boolean matches = true;
            for (String s : keysValues.keySet()) {
                if (!keysValues.get(s).equals(d.getMetadataValue(s))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                list.add(d);
            }
        }
        return list;
    }

    @Override
    public List<Document> searchByKeywordAndMetadata(String keyword, Map<String, String> keysValues) {
        List<Document> doubleSearch = new ArrayList<>();
        List<Document> keywordSearch = search(keyword);
        List<Document> metadataSearch = searchByMetadata(keysValues);
        if (keywordSearch.isEmpty() || metadataSearch.isEmpty()) {
            return doubleSearch;
        }
        for (Document d : keywordSearch) {
            if (metadataSearch.contains(d)) {
              doubleSearch.add(d);
            }
        }
        return doubleSearch;
    }

    @Override
    public List<Document> searchByPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        List<Document> doubleSearch = new ArrayList<>();
        List<Document> prefixSearch = searchByPrefix(keywordPrefix);
        List<Document> metadataSearch = searchByMetadata(keysValues);
        if (prefixSearch.isEmpty() || metadataSearch.isEmpty()) {
            return doubleSearch;
        }
        for (Document d : prefixSearch) {
            if (metadataSearch.contains(d)) {
                doubleSearch.add(d);
            }
        }
        return doubleSearch;
    }

    @Override
    public Set<URI> deleteAllWithMetadata(Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> metadataSearch = searchByMetadata(keysValues);
        for (Document doc : metadataSearch) {
            Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
            GenericCommand<URI> genericCommand = new GenericCommand<>(doc.getKey(), consumer);
            if (metadataSearch.size() == 1) {
                this.commandStack.push(genericCommand);
            }
            command.addCommand(genericCommand);
        }
        if (metadataSearch.size() != 1) {
            this.commandStack.push(command);
        }
        return deletionSet(metadataSearch);
    }

    @Override
    public Set<URI> deleteAllWithKeywordAndMetadata(String keyword, Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> keyMetaSearch = searchByKeywordAndMetadata(keyword, keysValues);
        for (Document doc : keyMetaSearch) {
            Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
            GenericCommand<URI> genericCommand = new GenericCommand<>(doc.getKey(), consumer);
            if (keyMetaSearch.size() == 1) {
                this.commandStack.push(genericCommand);
            }
            command.addCommand(genericCommand);
        }
        if (keyMetaSearch.size() != 1) {
            this.commandStack.push(command);
        }
        return deletionSet(keyMetaSearch);
    }

    @Override
    public Set<URI> deleteAllWithPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> preMetaSearch = searchByPrefixAndMetadata(keywordPrefix, keysValues);
        for (Document doc : preMetaSearch) {
            Consumer<URI> consumer = uri -> this.documents.put(uri, doc);
            GenericCommand<URI> genericCommand = new GenericCommand<>(doc.getKey(), consumer);
            if (preMetaSearch.size() == 1) {
                this.commandStack.push(genericCommand);
            }
            command.addCommand(genericCommand);
        }
        if (preMetaSearch.size() != 1) {
            this.commandStack.push(command);
        }
        return deletionSet(preMetaSearch);
    }

    private Set<URI> deletionSet(Collection<Document> coll) {
        Set<URI> faded = new HashSet<>();
        for (Document d : coll) {
            for (String s : d.getWords()){
                this.docTrie.delete(s, d);
            }
            faded.add(d.getKey());
            this.documents.put(d.getKey(), null);
        }
        return faded;
    }
}
