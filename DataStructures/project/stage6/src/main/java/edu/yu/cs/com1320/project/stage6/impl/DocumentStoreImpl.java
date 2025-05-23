package edu.yu.cs.com1320.project.stage6.impl;

import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.Stack;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.stage6.*;
import edu.yu.cs.com1320.project.undo.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

public class DocumentStoreImpl implements DocumentStore {
    // variables
    private final BTree<URI, Document> documents;
    private final Stack<Undoable> commandStack;
    private final Trie<DocumentReference> docTrie;
    private final MinHeap<DocumentReference> docMinHeap;
    private int maxDocCount;
    private int maxDocBytes;
    private final Map<URI, Boolean> areDocsInMemory;

    private class DocumentReference implements Comparable<DocumentReference> {
        private final URI uri;

        public DocumentReference(URI uri) {
            this.uri = uri;
        }

        public URI getUri() {
            return this.uri;
        }

        private long getLastUseTime() {
            Document doc = documents.get(uri);
            return doc == null ? Long.MIN_VALUE : doc.getLastUseTime();
        }

        public int getWordCount(String word) {
            Document doc = documents.get(uri);
            return documents.get(uri) == null ? 0 : doc.wordCount(word);
        }

        @Override
        public int compareTo(DocumentReference other) {
            return Long.compare(this.getLastUseTime(), other.getLastUseTime());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DocumentReference)) return false;
            return this.uri.equals(((DocumentReference) o).uri);
        }

        @Override
        public int hashCode() {
            return this.uri.hashCode();
        }
    }

    // constructor
    public DocumentStoreImpl () {
        this.documents = new BTreeImpl<>();
        documents.setPersistenceManager(new DocumentPersistenceManager(null));
        this.commandStack = new StackImpl<>();
        this.docTrie = new TrieImpl<>();
        this.docMinHeap = new MinHeapImpl<>();
        this.maxDocCount = -1;
        this.maxDocBytes = -1;
        this.areDocsInMemory = new HashMap<>();
    }

    public DocumentStoreImpl (File baseDir) {
        this.documents = new BTreeImpl<>();
        documents.setPersistenceManager(new DocumentPersistenceManager(baseDir));
        this.commandStack = new StackImpl<>();
        this.docTrie = new TrieImpl<>();
        this.docMinHeap = new MinHeapImpl<>();
        this.maxDocCount = -1;
        this.maxDocBytes = -1;
        this.areDocsInMemory = new HashMap<>();
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
        this.docMinHeap.insert(new DocumentReference(newDoc.getKey()));
        this.documents.put(uri, newDoc);
        if (prevDoc == null) {
            areDocsInMemory.put(uri, true);
        }
        checkLimits();
        return prevDocHashCode;
    }

    private Document docCreate(InputStream input, URI uri, DocumentFormat format) throws IOException {
        Document document = null;
        if (format == DocumentFormat.TXT) {
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            String result = text.replaceAll("[^a-zA-Z0-9 ]", "");
            String[] textAsArray = result.split("\\s+");
            HashMap<String, Integer> map = new HashMap<>();
            for (String s : textAsArray) {
                map.put(s, map.getOrDefault(s, 0) + 1);
            }
            document = new DocumentImpl(uri, text, map);
        }
        if (format == DocumentFormat.BINARY) {
            byte[] binaryData = input.readAllBytes();
            document = new DocumentImpl(uri, binaryData);
        }
        return document;
    }

    private void undoAndTrie(int prevDocHashCode, Document prevDoc, URI uri, Document newDoc){
        DocumentReference ref = new DocumentReference(newDoc.getKey());
        for (String s : newDoc.getWords()) {
            this.docTrie.put(s, ref);
        }
        Consumer<URI> consumer = url -> {
            newDoc.setLastUseTime(-1);
            docMinHeap.reHeapify(new DocumentReference(newDoc.getKey()));
            docMinHeap.remove();
            this.documents.put(url, prevDoc);
            DocumentReference refNew = new DocumentReference(newDoc.getKey());
            for (String s : newDoc.getWords()) {
                this.docTrie.delete(s, refNew);
            }
            if (prevDoc != null) {
                DocumentReference refPrev = new DocumentReference(prevDoc.getKey());
                for (String s : prevDoc.getWords()) {
                    this.docTrie.put(s, refPrev);
                }
                prevDoc.setLastUseTime(System.nanoTime());
                this.docMinHeap.insert(new DocumentReference(prevDoc.getKey()));
            } else {
                areDocsInMemory.remove(url);
            }
        };
        Undoable command = new GenericCommand<>(uri, consumer);
        this.commandStack.push(command);
        if (prevDocHashCode != 0) {
            DocumentReference refPrev = new DocumentReference(prevDoc.getKey());
            for (String s : prevDoc.getWords()) {
                this.docTrie.delete(s, refPrev);
            }
        }
    }

    // get doc
    @Override
    public Document get(URI url) {
        Document doc = this.documents.get(url);
        if (doc != null) {
            doc.setLastUseTime(System.nanoTime());
            this.docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
            areDocsInMemory.put(doc.getKey(), true);
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
        Undoable command = getCommand(url, prevDoc);
        this.commandStack.push(command);
        DocumentReference ref = new DocumentReference(prevDoc.getKey());
        for (String s : prevDoc.getWords()) {
            this.docTrie.delete(s, ref);
       }
       prevDoc.setLastUseTime(-1);
       docMinHeap.reHeapify(new DocumentReference(prevDoc.getKey()));
       docMinHeap.remove();
       this.documents.put(url, null);
       areDocsInMemory.remove(url);
       return true;
    }

    private Undoable getCommand(URI url, Document prevDoc) {
        Set<String> words = prevDoc.getWords();
        Consumer<URI> consumer = uri -> {
            this.documents.put(uri, prevDoc);
            areDocsInMemory.put(uri, true);
            DocumentReference ref = new DocumentReference(prevDoc.getKey());
            for (String s : words) {
                this.docTrie.put(s, ref);
            }
            prevDoc.setLastUseTime(System.nanoTime());
            docMinHeap.insert(new DocumentReference(prevDoc.getKey()));
            checkLimits();
        };
        return new GenericCommand<>(url, consumer);
    }

    private GenericCommand<URI> getUriGenericCommand(Document doc) {
        Consumer<URI> consumer = uri -> {
            this.documents.put(uri, doc);
            areDocsInMemory.put(uri, true);
            DocumentReference ref = new DocumentReference(doc.getKey());
            for (String s : doc.getWords()) {
                this.docTrie.put(s, ref);
            }
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.insert(new DocumentReference(doc.getKey()));
            checkLimits();
        };
        return new GenericCommand<>(doc.getKey(), consumer);
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
        Comparator<DocumentReference> comp = Comparator.comparingInt((DocumentReference docRef) -> docRef.getWordCount(keyword)).reversed();
        List<DocumentReference> refs = this.docTrie.getSorted(keyword, comp);
        if (refs == null) {
            return List.of();
        }
        List<Document> docs = new ArrayList<>();
        for (DocumentReference ref : refs) {
            Document doc = documents.get(ref.getUri());
            if (doc != null) {
                doc.setLastUseTime(System.nanoTime());
                this.docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
                docs.add(doc);
            }
        }
        checkLimits();
        return docs;
    }

    @Override
    public List<Document> searchByPrefix(String keywordPrefix) {
        Comparator<DocumentReference> comp = Comparator.comparingInt((DocumentReference docRef) -> docRef.getWordCount(keywordPrefix)).reversed();
        List<DocumentReference> refs = this.docTrie.getAllWithPrefixSorted(keywordPrefix, comp);
        if (refs == null) {
            return List.of();
        }
        List<Document> docs = new ArrayList<>();
        for (DocumentReference ref : refs) {
            Document doc = documents.get(ref.getUri());
            if (doc != null) {
                doc.setLastUseTime(System.nanoTime());
                this.docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
                docs.add(doc);
            }
        }
        checkLimits();
        return docs;
    }

    @Override
    public Set<URI> deleteAll(String keyword) {
        CommandSet<URI> command = new CommandSet<>();
        Set<DocumentReference> hits = this.docTrie.deleteAll(keyword);
        Set<Document> docs = new HashSet<>();
        for (DocumentReference ref : hits) {
            Document doc = documents.get(ref.getUri());
            if (doc != null) {
                docs.add(doc);
                GenericCommand<URI> genericCommand = getUriGenericCommand(doc);
                if (hits.size() == 1) {
                    this.commandStack.push(genericCommand);
                }
                command.addCommand(genericCommand);
            }
        }
        if (hits.size() > 1) {
            this.commandStack.push(command);
        }
        return deletionSet(docs);
    }

    @Override
    public Set<URI> deleteAllWithPrefix(String keywordPrefix) {
        CommandSet<URI> command = new CommandSet<>();
        Set<DocumentReference> hits = this.docTrie.deleteAllWithPrefix(keywordPrefix);
        Set<Document> docs = new HashSet<>();
        for (DocumentReference ref : hits) {
            Document doc = documents.get(ref.getUri());
            if (doc != null) {
                docs.add(doc);
                GenericCommand<URI> genericCommand = getUriGenericCommand(doc);
                if (hits.size() == 1) {
                    this.commandStack.push(genericCommand);
                }
                command.addCommand(genericCommand);
            }
        }
        if (hits.size() > 1) {
            this.commandStack.push(command);
        }
        return deletionSet(docs);
    }

    @Override
    public List<Document> searchByMetadata(Map<String, String> keysValues) {
        List<Document> docs = new ArrayList<>();
        for (URI uri : areDocsInMemory.keySet()) {
            Document d = documents.get(uri);
            boolean matches = true;
            for (String s : keysValues.keySet()) {
                if (!keysValues.get(s).equals(d.getMetadataValue(s))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                docs.add(d);
                areDocsInMemory.put(uri, true);
            } else {
                try {
                    documents.moveToDisk(uri);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (docs.isEmpty()) {
            return List.of();
        }
        for (Document doc : docs) {
            doc.setLastUseTime(System.nanoTime());
            docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
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
            docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
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
            docMinHeap.reHeapify(new DocumentReference(doc.getKey()));
        }
        return docs;
    }

    @Override
    public Set<URI> deleteAllWithMetadata(Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> metadataSearch = searchByMetadata(keysValues);
        for (Document doc : metadataSearch) {
            GenericCommand<URI> genericCommand = getUriGenericCommand(doc);
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
            GenericCommand<URI> genericCommand = getUriGenericCommand(doc);
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

    private Set<URI> deletionSet(Collection<Document> coll) {
        Set<URI> faded = new HashSet<>();
        for (Document d : coll) {
            DocumentReference ref = new DocumentReference(d.getKey());
            for (String s : d.getWords()){
                this.docTrie.delete(s, ref);
            }
            faded.add(d.getKey());
            Document docToUpdate = this.documents.put(d.getKey(), null);
            docToUpdate.setLastUseTime(-1);
            docMinHeap.reHeapify(new DocumentReference(docToUpdate.getKey()));
            docMinHeap.remove();
            areDocsInMemory.remove(d.getKey());
        }
        return faded;
    }

    @Override
    public Set<URI> deleteAllWithPrefixAndMetadata(String keywordPrefix, Map<String, String> keysValues) {
        CommandSet<URI> command = new CommandSet<>();
        List<Document> preMetaSearch = searchByPrefixAndMetadata(keywordPrefix, keysValues);
        for (Document doc : preMetaSearch) {
            GenericCommand<URI> genericCommand = getUriGenericCommand(doc);
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
        while (docsInMemorySize() > maxDocCount) {
            DocumentReference ref = this.docMinHeap.remove();
            try {
                documents.moveToDisk(ref.getUri());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            areDocsInMemory.put(ref.getUri(), false);
        }
    }

    private void overDocBytesCheck() {
        int i = 0;
        for (URI uri : docsInMemory()) {
            Document d = documents.get(uri);
            if (d.getDocumentTxt() != null) {
                i += d.getDocumentTxt().getBytes().length;
            } else {
                i += d.getDocumentBinaryData().length;
            }
            if (!areDocsInMemory.get(d.getKey())) {
                try {
                    documents.moveToDisk(d.getKey());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        while (i > this.maxDocBytes) {
            DocumentReference ref = this.docMinHeap.remove();
            Document deletedDoc = this.documents.get(ref.getUri());
            if (deletedDoc.getDocumentTxt() != null) {
                i -= deletedDoc.getDocumentTxt().getBytes().length;
            } else {
                i -= deletedDoc.getDocumentBinaryData().length;
            }
            try {
                documents.moveToDisk(ref.getUri());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            areDocsInMemory.put(ref.getUri(), false);
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

    private int docsInMemorySize() {
        int i = 0;
        for(boolean b : areDocsInMemory.values()) {
            if (b) {
                i++;
            }
        }
        return i;
    }

    private Set<URI> docsInMemory() {
        Set<URI> uriSet = new HashSet<>();
        for(URI uri : areDocsInMemory.keySet()) {
            if (areDocsInMemory.get(uri)) {
                uriSet.add(uri);
            }
        }
        return uriSet;
    }
}