package edu.yu.cs.com1320.project.stage5.impl;

import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.Stack;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.stage5.*;
import edu.yu.cs.com1320.project.undo.*;

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
    private final MinHeap<Document> docMinHeap;
    private int maxDocCount;
    private int maxDocBytes;
    // constructor
    public DocumentStoreImpl () {
        this.documents = new HashTableImpl<>();
        this.commandStack = new StackImpl<>();
        this.docTrie = new TrieImpl<>();
        this.docMinHeap = new MinHeapImpl<>();
        this.maxDocCount = -1;
        this.maxDocBytes = -1;
    }

    // set metadata
    @Override
    public String setMetadata(URI uri, String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or blank");
        }
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
        Document prevDoc = get(uri);
        int prevDocHashCode = prevDoc != null ? prevDoc.hashCode() : 0;
        if (input == null) {
            delete(uri);
            return prevDocHashCode;
        }
        Document newDoc = docCreate(input, uri, format);
        undoAndTrie(prevDocHashCode, prevDoc, uri, newDoc);
        newDoc.setLastUseTime(System.nanoTime());
        this.docMinHeap.insert(newDoc);
        this.documents.put(uri, newDoc);
        checkLimits();
        return prevDocHashCode;
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

    private void undoAndTrie(int prevDocHashCode, Document prevDoc, URI uri, Document newDoc){
        for (String s : newDoc.getWords()) {
            this.docTrie.put(s,newDoc);
        }
        Consumer<URI> consumer = url -> {
            newDoc.setLastUseTime(-1);
            docMinHeap.reHeapify(newDoc);
            docMinHeap.remove();
            this.documents.put(url, prevDoc);
            for (String s : newDoc.getWords()) {
                this.docTrie.delete(s,newDoc);
            }
            if (prevDoc != null) {
                for (String s : prevDoc.getWords()) {
                    this.docTrie.put(s, prevDoc);
                }
                prevDoc.setLastUseTime(System.nanoTime());
                this.docMinHeap.insert(prevDoc);
            }
        };
        Undoable command = new GenericCommand<>(uri, consumer);
        this.commandStack.push(command);
        if (prevDocHashCode != 0) {
            for (String s : prevDoc.getWords()) {
                this.docTrie.delete(s,prevDoc);
            }
        }
    }

    // get doc
    @Override
    public Document get(URI url) {
        Document doc = this.documents.get(url);
        if (doc != null) {
            doc.setLastUseTime(System.nanoTime());
            this.docMinHeap.reHeapify(doc);
        }
        return doc;
    }

    // deleter
    @Override
    public boolean delete(URI url) {
        Document prevDoc = this.documents.get(url);
        if (prevDoc == null) {
           return false;
        }
        Set<String> words = prevDoc.getWords();
        Consumer<URI> consumer = uri -> {
            this.documents.put(uri, prevDoc);
            for (String s : words) {
                this.docTrie.put(s, prevDoc);
            }
            prevDoc.setLastUseTime(System.nanoTime());
            docMinHeap.insert(prevDoc);
            checkLimits();
        };
       Undoable command = new GenericCommand<>(url, consumer);
       this.commandStack.push(command);
       for (String s : prevDoc.getWords()) {
            this.docTrie.delete(s,prevDoc);
       }
       prevDoc.setLastUseTime(-1);
       docMinHeap.reHeapify(prevDoc);
       docMinHeap.remove();
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
        Stack<Undoable> temp = new StackImpl<>();
        findCommandToUndo(temp, url);
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

    private void findCommandToUndo(Stack<Undoable> temp, URI url) {
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
                if (genericCommand.getTarget().equals(url)) {
                    break;
                }
            }
            temp.push(this.commandStack.pop());
        }
    }

    @Override
    public List<Document> search(String keyword) {
        Comparator<Document> comp = Comparator.comparingInt((Document doc) -> doc.wordCount(keyword)).reversed();
        List<Document> docs = this.docTrie.getSorted(keyword, comp);
        if (docs == null) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(doc);
        }
        return docs;
    }

    @Override
    public List<Document> searchByPrefix(String keywordPrefix) {
        Comparator<Document> comp = Comparator.comparingInt((Document doc) -> doc.wordCount(keywordPrefix)).reversed();
        List<Document> docs = this.docTrie.getAllWithPrefixSorted(keywordPrefix, comp);
        if (docs == null) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(doc);
        }
        return docs;
    }

    @Override
    public Set<URI> deleteAll(String keyword) {
        CommandSet<URI> command = new CommandSet<>();
        Set<Document> hits = this.docTrie.deleteAll(keyword);
        for (Document doc : hits) {
            Consumer<URI> consumer = uri -> {
                this.documents.put(uri, doc);
                for (String s : doc.getWords()) {
                    this.docTrie.put(s, doc);
                }
                doc.setLastUseTime(System.nanoTime());
                docMinHeap.insert(doc);
                checkLimits();
            };
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
            Consumer<URI> consumer = uri -> {
                this.documents.put(uri, doc);
                for (String s : doc.getWords()) {
                    this.docTrie.put(s, doc);
                }
                doc.setLastUseTime(System.nanoTime());
                docMinHeap.insert(doc);
                checkLimits();
            };
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
        List<Document> docs = new ArrayList<>();
        for (Document d : this.documents.values()) {
            boolean matches = true;
            for (String s : keysValues.keySet()) {
                if (!keysValues.get(s).equals(d.getMetadataValue(s))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                docs.add(d);
            }
        }
        if (docs.isEmpty()) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(doc);
        }
        return docs;
    }

    @Override
    public List<Document> searchByKeywordAndMetadata(String keyword, Map<String, String> keysValues) {
        List<Document> docs = new ArrayList<>();
        List<Document> keywordSearch = search(keyword);
        List<Document> metadataSearch = searchByMetadata(keysValues);
        if (keywordSearch.isEmpty() || metadataSearch.isEmpty()) {
            return docs;
        }
        for (Document d : keywordSearch) {
            if (metadataSearch.contains(d)) {
              docs.add(d);
            }
        }
        if (docs.isEmpty()) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(doc);
        }
        return docs;
    }

    @Override
    public List<Document> searchByPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        List<Document> docs = new ArrayList<>();
        List<Document> prefixSearch = searchByPrefix(keywordPrefix);
        List<Document> metadataSearch = searchByMetadata(keysValues);
        if (prefixSearch.isEmpty() || metadataSearch.isEmpty()) {
            return docs;
        }
        for (Document d : prefixSearch) {
            if (metadataSearch.contains(d)) {
                docs.add(d);
            }
        }
        if (docs.isEmpty()) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(doc);
        }
        return docs;
    }

    @Override
    public Set<URI> deleteAllWithMetadata(Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> metadataSearch = searchByMetadata(keysValues);
        for (Document doc : metadataSearch) {
            Consumer<URI> consumer = uri -> {
                this.documents.put(uri, doc);
                for (String s : doc.getWords()) {
                    this.docTrie.put(s, doc);
                }
                doc.setLastUseTime(System.nanoTime());
                docMinHeap.insert(doc);
                checkLimits();
            };
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
            Consumer<URI> consumer = uri -> {
                this.documents.put(uri, doc);
                for (String s : doc.getWords()) {
                    this.docTrie.put(s, doc);
                }
                doc.setLastUseTime(System.nanoTime());
                docMinHeap.insert(doc);
                checkLimits();
            };
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
            Consumer<URI> consumer = uri -> {
                this.documents.put(uri, doc);
                for (String s : doc.getWords()) {
                    this.docTrie.put(s, doc);
                }
                doc.setLastUseTime(System.nanoTime());
                docMinHeap.insert(doc);
                checkLimits();
            };
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
            Document docToUpdate = this.documents.get(d.getKey());
            docToUpdate.setLastUseTime(-1);
            docMinHeap.reHeapify(docToUpdate);
            docMinHeap.remove();
            this.documents.put(d.getKey(), null);
        }
        return faded;
    }

    @Override
    public void setMaxDocumentCount(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit is less than 1");
        }
        this.maxDocCount = limit;
        overDocCountCheck();
    }

    @Override
    public void setMaxDocumentBytes(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit is less than 1");
        }
        this.maxDocBytes = limit;
        overDocBytesCheck();
    }

    private void overDocCountCheck() {
        while (documents.size() > maxDocCount) {
            Document deletedDoc = docMinHeap.remove();
            Stack<Undoable> temp = new StackImpl<>();
            while (commandStack.peek() != null) {
                removeFromStackEntirely(temp, deletedDoc.getKey());
            }
            while (temp.peek() != null) {
                this.commandStack.push(temp.pop());
            }
            for (String s : deletedDoc.getWords()){
                this.docTrie.delete(s, deletedDoc);
            }
            documents.put(deletedDoc.getKey(), null);
        }
    }

    private void overDocBytesCheck() {
        int i = 0;
        for (Document d : this.documents.values()) {
            if (d.getDocumentTxt() != null) {
                i += d.getDocumentTxt().getBytes().length;
            } else {
                i += d.getDocumentBinaryData().length;
            }
        }
        while (i > this.maxDocBytes) {
            Document deletedDoc = docMinHeap.remove();
            if (deletedDoc.getDocumentTxt() != null) {
                i -= deletedDoc.getDocumentTxt().getBytes().length;
            } else {
                i -= deletedDoc.getDocumentBinaryData().length;
            }
            Stack<Undoable> temp = new StackImpl<>();
            while (commandStack.peek() != null) {
                removeFromStackEntirely(temp, deletedDoc.getKey());
            }
            while (temp.peek() != null) {
                this.commandStack.push(temp.pop());
            }
            for (String s : deletedDoc.getWords()){
                this.docTrie.delete(s, deletedDoc);
            }
            documents.put(deletedDoc.getKey(), null);
        }
    }

    private void removeFromStackEntirely(Stack<Undoable> temp, URI url) {
        findCommandToUndo(temp, url);
        if (this.commandStack.peek() instanceof CommandSet) {
            @SuppressWarnings("unchecked")
            CommandSet<URI> commandSet = (CommandSet<URI>) this.commandStack.peek();
            CommandSet<URI> newCommandSet = new CommandSet<>();
            for (GenericCommand<URI> g : commandSet) {
                if (g.getTarget() != url) {
                    newCommandSet.addCommand(g);
                }
            }
            commandStack.pop();
            temp.push(newCommandSet);
        } else try {
            this.commandStack.pop();
        } catch (NullPointerException e) {
            throw new IllegalStateException("command stack is empty");
        }
    }

    private void checkLimits() {
        if (maxDocCount != -1) {
            overDocCountCheck();
        }
        if (maxDocBytes != -1) {
            overDocBytesCheck();
        }
    }
}