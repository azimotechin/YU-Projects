package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.BTree;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import edu.yu.cs.com1320.project.stage6.Document;
import edu.yu.cs.com1320.project.stage6.impl.DocumentImpl;
import edu.yu.cs.com1320.project.stage6.impl.DocumentPersistenceManager;

import org.junit.jupiter.api.*;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BTreeImplTest {

    private BTree<URI, Document> bTree;
    private File baseDir;
    private Map<String, Integer> wordCountMap;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setUp() {
        this.baseDir = new File("src/test/resources/doc-test");
        if (baseDir.exists()) {
            deleteDir(baseDir);
        }
        baseDir.mkdirs();
        this.bTree = new BTreeImpl<>();
        this.bTree.setPersistenceManager(new DocumentPersistenceManager(baseDir));
        this.wordCountMap = new HashMap<>();
    }

    @AfterEach
    public void cleanUp() {
        deleteDir(baseDir);
        wordCountMap.clear();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File f : Objects.requireNonNull(dir.listFiles())) {
                deleteDir(f);
            }
        }
        dir.delete();
    }

    private URI makeUri(String s) {
        return URI.create("http://test.com/" + s);
    }

    private Document makeDoc(String uriSuffix, String txt) {
        URI uri = makeUri(uriSuffix);
        Document doc = new DocumentImpl(uri, txt, null);
        String[] words = txt.split("\\s+");
        for (String word : words) {
            wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
        }
        return doc;
    }

    @Test
    public void testPutAndGetSimple() {
        URI uri = makeUri("doc1");
        Document doc = makeDoc("doc1", "hello world");
        bTree.put(uri, doc);
        Document result = bTree.get(uri);
        assertEquals("hello world", result.getDocumentTxt());
    }

    @Test
    public void testPutOverwrite() {
        URI uri = makeUri("doc1");
        Document doc1 = makeDoc("doc1", "text 1");
        Document doc2 = makeDoc("doc1", "text 2");
        bTree.put(uri, doc1);
        bTree.put(uri, doc2);
        Document result = bTree.get(uri);
        assertEquals("text 2", result.getDocumentTxt());
    }

    @Test
    public void testMoveToDiskThenGet() throws Exception {
        URI uri = makeUri("doc1");
        Document doc = makeDoc("doc1", "disk data");
        bTree.put(uri, doc);
        bTree.moveToDisk(uri);
        Document result = bTree.get(uri);
        assertEquals("disk data", result.getDocumentTxt());
        File file = new File(baseDir, "test_com_doc1.json");
        assertFalse(file.exists());
    }

    @Test
    public void testGetNonExistentKeyReturnsNull() {
        URI uri = makeUri("missing");
        assertNull(bTree.get(uri));
    }

    @Test
    public void testMoveToDiskOnNullValueDoesNothing() throws Exception {
        URI uri = makeUri("null");
        bTree.moveToDisk(uri);
    }

    @Test
    public void testMultipleDocumentsInsertAndRetrieve() {
        for (int i = 1; i <= 99; i++) {
            URI uri = makeUri("doc" + i);
            Document doc = makeDoc("doc" + i, "text " + i);
            bTree.put(uri, doc);
        }
        for (int i = 1; i <= 99; i++) {
            URI uri = makeUri("doc" + i);
            Document result = bTree.get(uri);
            assertNotNull(result, "Document should not be null for doc" + i);
            assertEquals("text " + i, result.getDocumentTxt());
        }
    }

    @Test
    public void testPersistenceAfterMoveToDiskAndMemoryOverwrite() throws Exception {
        URI uri = makeUri("doc");
        Document original = makeDoc("doc", "original");
        Document replacement = makeDoc("doc", "replacement");
        bTree.put(uri, original);
        bTree.moveToDisk(uri);
        bTree.put(uri, replacement);
        File file = new File(baseDir, "test_com_doc.json");
        assertFalse(file.exists());
        Document result = bTree.get(uri);
        assertEquals("replacement", result.getDocumentTxt());
    }

    @Test
    public void testWordCountMap() {
        String txt = "hello hello world";
        makeDoc("doc1", txt);
        assertEquals(2, wordCountMap.get("hello"));
        assertEquals(1, wordCountMap.get("world"));
        assertNull(wordCountMap.get("nonexistent"));
    }
}