package edu.yu.cs.com1320.project.stage6.impl;

import edu.yu.cs.com1320.project.stage6.DocumentStore;
import edu.yu.cs.com1320.project.stage6.Document;
import edu.yu.cs.com1320.project.stage6.DocumentStore.DocumentFormat;

import org.junit.jupiter.api.*;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentStoreImplTest {

    private DocumentStore store;
    private File baseDir;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setup() {
        baseDir = new File("src/test/resources/doc-test");
        baseDir.mkdir();
        store = new DocumentStoreImpl(baseDir);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void cleanup() {
        for (File file : Objects.requireNonNull(baseDir.listFiles())) {
            file.delete();
        }
        baseDir.delete();
    }

    private InputStream stringToInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }

    private InputStream bytesToInputStream(byte[] bytes) {
        return new ByteArrayInputStream(bytes);
    }

    @Test
    public void testPutAndGetTxtDocument() throws IOException {
        URI uri = URI.create("http://www.example.com/docs/test1");
        String txt = "hello world hello!";
        store.put(stringToInputStream(txt), uri, DocumentFormat.TXT);
        Document doc = store.get(uri);
        assertNotNull(doc);
        assertEquals(txt, doc.getDocumentTxt());
        assertEquals(2, doc.wordCount("hello"));
        assertEquals(1, doc.wordCount("world"));
    }

    @Test
    public void testPutBinaryDocument() throws IOException {
        URI uri = URI.create("http://www.example.com/docs/binary");
        byte[] data = {1, 2, 3, 4};
        store.put(bytesToInputStream(data), uri, DocumentFormat.BINARY);
        Document doc = store.get(uri);
        assertNotNull(doc);
        assertArrayEquals(data, doc.getDocumentBinaryData());
    }

    @Test
    public void testDeleteDocument() throws IOException {
        URI uri = URI.create("http://www.example.com/docs/delete-me");
        String text = "Delete this text";
        store.put(stringToInputStream(text), uri, DocumentFormat.TXT);
        assertTrue(store.delete(uri));
        assertNull(store.get(uri));
    }

    @Test
    public void testUndoPut() throws IOException {
        URI uri = URI.create("http://www.example.com/docs/undo-put");
        store.put(stringToInputStream("text1"), uri, DocumentFormat.TXT);
        store.undo();
        assertNull(store.get(uri));
    }

    @Test
    public void testUndoDelete() throws IOException {
        URI uri = URI.create("doc://undo-delete");
        store.put(stringToInputStream("undo this delete"), uri, DocumentFormat.TXT);
        store.delete(uri);
        store.undo();
        assertNotNull(store.get(uri));
    }

    @Test
    public void testSetAndGetMetadata() throws IOException {
        URI uri = URI.create("doc://meta");
        store.put(stringToInputStream("metadata"), uri, DocumentFormat.TXT);
        assertNull(store.setMetadata(uri, "author", "john"));
        assertEquals("john", store.getMetadata(uri, "author"));
    }

    @Test
    public void testSearchByKeyword() throws IOException {
        store.put(stringToInputStream("alpha beta gamma"), URI.create("doc://1"), DocumentFormat.TXT);
        store.put(stringToInputStream("alpha alpha beta"), URI.create("doc://2"), DocumentFormat.TXT);
        List<Document> results = store.search("alpha");
        assertEquals(2, results.size());
        assertTrue(results.get(0).wordCount("alpha") >= results.get(1).wordCount("alpha"));
    }

    @Test
    public void testSearchByPrefix() throws IOException {
        store.put(stringToInputStream("jumped jumping jumps"), URI.create("doc://prefix1"), DocumentFormat.TXT);
        store.put(stringToInputStream("jump jump jump"), URI.create("doc://prefix2"), DocumentFormat.TXT);
        List<Document> results = store.searchByPrefix("jump");
        assertEquals(2, results.size());
    }

    @Test
    public void testDocumentMovedToDisk() throws IOException {
        store.setMaxDocumentCount(1);
        URI uri1 = URI.create("doc://disk-test-1.com/bro");
        URI uri2 = URI.create("doc://disk-test-2.com/entity");
        store.put(stringToInputStream("first doc"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("second doc"), uri2, DocumentFormat.TXT);
        File expectedFile = new File(baseDir, "disk-test-1.com" + File.separator + "bro.json");
        assertTrue(expectedFile.exists(), "Document should be on disk after eviction");
    }


    @Test
    public void testMemoryLimitDocumentBytes() throws IOException {
        store.setMaxDocumentBytes(10);
        URI uri1 = URI.create("doc://bytes1");
        URI uri2 = URI.create("doc://bytes2");
        store.put(stringToInputStream("small"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("document of size large"), uri2, DocumentFormat.TXT);
        File file = new File(baseDir, "bytes1");
        assertTrue(file.exists());
        assertNotNull(store.get(uri2));
    }

    @Test
    public void testRestoreFromDiskAfterMemoryOverflow() throws IOException {
        store.setMaxDocumentCount(1);
        URI uri1 = URI.create("doc://restore1");
        URI uri2 = URI.create("doc://restore2");
        store.put(stringToInputStream("first document"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("second document"), uri2, DocumentFormat.TXT);
        Document d1 = store.get(uri1);
        assertNotNull(d1);
        assertEquals("first document", d1.getDocumentTxt());
    }

    @Test
    public void testDeleteAllAndUndo() throws IOException {
        URI uri1 = URI.create("doc://bulk1");
        URI uri2 = URI.create("doc://bulk2");
        store.put(stringToInputStream("cat dog"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("cat mouse"), uri2, DocumentFormat.TXT);
        Set<URI> deleted = store.deleteAll("cat");
        assertTrue(deleted.contains(uri1));
        assertTrue(deleted.contains(uri2));
        store.undo();
        assertNotNull(store.get(uri1));
        assertNotNull(store.get(uri2));
    }

    @Test
    public void testDeleteAllWithPrefixAndUndo() throws IOException {
        URI uri1 = URI.create("doc://prebulk1");
        URI uri2 = URI.create("doc://prebulk2");
        store.put(stringToInputStream("sing sang song"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("singer singe sing"), uri2, DocumentFormat.TXT);
        Set<URI> deleted = store.deleteAllWithPrefix("sing");
        assertEquals(2, deleted.size());
        store.undo();
        assertNotNull(store.get(uri1));
        assertNotNull(store.get(uri2));
    }

    @Test
    public void testUndoPutNewDocument() throws IOException {
        URI uri = URI.create("doc://undo-new");
        store.put(stringToInputStream("first version"), uri, DocumentFormat.TXT);
        store.undo();
        assertNull(store.get(uri));
    }

    @Test
    public void testUndoOverwriteDocument() throws IOException {
        URI uri = URI.create("doc://undo-overwrite");
        store.put(stringToInputStream("version one"), uri, DocumentFormat.TXT);
        store.put(stringToInputStream("version two"), uri, DocumentFormat.TXT);
        store.undo();
        assertEquals("version one", store.get(uri).getDocumentTxt());
    }

    @Test
    public void testUndoDeleteSpecificURI() throws IOException {
        URI uri = URI.create("doc://undo-delete-specific");
        store.put(stringToInputStream("keep this"), uri, DocumentFormat.TXT);
        store.delete(uri);
        store.undo(uri);
        assertNotNull(store.get(uri));
    }

    @Test
    public void testUndoSetMetadata() throws IOException {
        URI uri = URI.create("doc://undo-metadata");
        store.put(stringToInputStream("metadata test"), uri, DocumentFormat.TXT);
        store.setMetadata(uri, "author", "john");
        store.setMetadata(uri, "author", "jane");
        store.undo(uri);
        assertEquals("john", store.getMetadata(uri, "author"));
        store.undo(uri);
        assertNull(store.getMetadata(uri, "author"));
    }

    @Test
    public void testUndoDeleteAllWithKeyword() throws IOException {
        URI uri1 = URI.create("doc://undo-keyword1");
        URI uri2 = URI.create("doc://undo-keyword2");
        store.put(stringToInputStream("target word"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("another target word"), uri2, DocumentFormat.TXT);
        store.deleteAll("target");
        store.undo();
        assertNotNull(store.get(uri1));
        assertNotNull(store.get(uri2));
    }

    @Test
    public void testUndoDeleteAllWithPrefix() throws IOException {
        URI uri1 = URI.create("doc://undo-prefix1");
        URI uri2 = URI.create("doc://undo-prefix2");
        store.put(stringToInputStream("prefixing is fun"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("prefixes help match"), uri2, DocumentFormat.TXT);
        store.deleteAllWithPrefix("prefix");
        store.undo();
        assertNotNull(store.get(uri1));
        assertNotNull(store.get(uri2));
    }

    @Test
    public void testUndoAfterEviction() throws IOException {
        store.setMaxDocumentCount(1);
        URI uri1 = URI.create("doc://evict1");
        URI uri2 = URI.create("doc://evict2");
        store.put(stringToInputStream("evicted doc"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("resident doc"), uri2, DocumentFormat.TXT);
        store.delete(uri1);
        store.undo(uri1);
        assertNotNull(store.get(uri1));
        assertEquals("evicted doc", store.get(uri1).getDocumentTxt());
    }

    @Test
    public void testUndoMultipleAndRedoOrder() throws IOException {
        URI uri1 = URI.create("doc://multi1");
        URI uri2 = URI.create("doc://multi2");
        store.put(stringToInputStream("doc1"), uri1, DocumentFormat.TXT);
        store.put(stringToInputStream("doc2"), uri2, DocumentFormat.TXT);
        store.delete(uri1);
        store.undo();
        assertNotNull(store.get(uri1));
        store.undo();
        assertNull(store.get(uri2));
        store.undo();
        assertNull(store.get(uri1));
    }
}