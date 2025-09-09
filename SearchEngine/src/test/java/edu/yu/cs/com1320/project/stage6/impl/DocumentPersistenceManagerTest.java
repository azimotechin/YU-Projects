package edu.yu.cs.com1320.project.stage6.impl;

import edu.yu.cs.com1320.project.stage6.Document;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentPersistenceManagerTest {

    private DocumentPersistenceManager dpm;
    private File baseDir;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setup() {
        baseDir = new File("src/test/resources/doc-test");
        baseDir.mkdirs();
        dpm = new DocumentPersistenceManager(baseDir);
    }

    @AfterEach
    public void cleanup() {
        deleteDirectory(baseDir);
    }

    @Test
    public void testSerializeAndDeserializeTextDocument() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/doc1");
        String text = "Hello world! hello test.";
        HashMap<String, Integer> wordMap = new HashMap<>();
        wordMap.put("Hello", 1);
        wordMap.put("hello", 1);
        wordMap.put("world", 1);
        wordMap.put("test", 1);
        Document originalDoc = new DocumentImpl(uri, text, wordMap);
        originalDoc.setMetadataValue("just so cool", "super cool");
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertEquals(originalDoc.getDocumentTxt(), deserializedDoc.getDocumentTxt());
        assertEquals(originalDoc.getMetadataValue("author"), deserializedDoc.getMetadataValue("author"));
        assertEquals(1, deserializedDoc.wordCount("Hello"));
        assertEquals(1, deserializedDoc.wordCount("hello"));
        assertEquals(1, deserializedDoc.wordCount("world"));
        assertEquals(1, deserializedDoc.wordCount("test"));
    }

    @Test
    public void testSerializeAndDeserializeBinaryDocument() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/binary");
        byte[] data = "some binary stuff".getBytes();
        Document originalDoc = new DocumentImpl(uri, data);
        originalDoc.setMetadataValue("format", "binary");
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertArrayEquals(data, deserializedDoc.getDocumentBinaryData());
        assertEquals("binary", deserializedDoc.getMetadataValue("format"));
        assertNull(deserializedDoc.getDocumentTxt());
    }

    @Test
    public void testDelete() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/foo/bar/doc");
        Document doc = new DocumentImpl(uri, "delete me", Map.of("delete", 1));
        dpm.serialize(uri, doc);
        File expectedFile = new File(baseDir, "example.com" + File.separator + "foo" + File.separator + "bar" + File.separator + "doc.json");
        assertTrue(expectedFile.exists(), "Serialized file should exist");
        boolean deleted = dpm.delete(uri);
        assertTrue(deleted, "File deletion should return true");
        assertNull(dpm.deserialize(uri), "Deserialization after delete should return null");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            for (File f : Objects.requireNonNull(dir.listFiles())) {
                deleteDirectory(f);
            }
        }
        dir.delete();
    }

    @Test
    public void testDeserializeNonExistentFile() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/nonexistent");
        Document deserializedDoc = dpm.deserialize(uri);
        assertNull(deserializedDoc);
    }

    @Test
    public void testSerializeAndDeserializeWithNullMetadata() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/nullmetadata");
        HashMap<String, Integer> wordMap = new HashMap<>();
        wordMap.put("Some", 1);
        wordMap.put("text", 1);
        Document originalDoc = new DocumentImpl(uri, "Some text", wordMap);
        originalDoc.setMetadata(null);
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertEquals(new HashMap<>(), deserializedDoc.getMetadata());
    }

    @Test
    public void testSerializeAndDeserializeLargeBinaryDocument() throws URISyntaxException, IOException {
        URI uri = new URI("http://example.com/largebinary");
        byte[] largeData = new byte[100000];
        Document originalDoc = new DocumentImpl(uri, largeData);
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertArrayEquals(largeData, deserializedDoc.getDocumentBinaryData());
    }

    @Test
    public void testSerializeAndDeserializeClasspathUri() throws Exception {
        URI uri = new URI("http://example.com/path");
        String text = "Classpath URI test content";
        HashMap<String, Integer> wordMap = new HashMap<>();
        wordMap.put("Classpath", 1);
        wordMap.put("URI", 1);
        wordMap.put("test", 1);
        Document originalDoc = new DocumentImpl(uri, text, wordMap);
        originalDoc.setMetadataValue("type", "classpath");
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertEquals(originalDoc.getDocumentTxt(), deserializedDoc.getDocumentTxt());
        assertEquals(originalDoc.getMetadataValue("type"), deserializedDoc.getMetadataValue("type"));
        assertEquals(1, deserializedDoc.wordCount("Classpath"));
        assertEquals(1, deserializedDoc.wordCount("URI"));
        assertEquals(1, deserializedDoc.wordCount("test"));
    }

    @Test
    public void testSerializeWithNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> dpm.serialize(null, new DocumentImpl(URI.create("http://a.com"), "text", Map.of())));
        assertThrows(IllegalArgumentException.class, () -> dpm.serialize(URI.create("http://a.com"), null));
    }

    @Test
    public void testDeserializeWithNullUri() {
        assertThrows(IllegalArgumentException.class, () -> dpm.deserialize(null));
    }

    @Test
    public void testDeleteWithNullUri() {
        assertThrows(IllegalArgumentException.class, () -> dpm.delete(null));
    }

    @Test
    public void testDeleteReturnsFalseOnMissingFile() throws Exception {
        URI uri = new URI("http://nonexistent.com/doc");
        assertFalse(dpm.delete(uri));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testSerializeFailsOnMkdirFailure() throws Exception {
        File badParent = new File(baseDir, "failDir.com");
        assertTrue(badParent.createNewFile());
        URI uri = new URI("http://failDir.com/doc");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        assertThrows(IOException.class, () -> dpm.serialize(uri, doc));
        badParent.delete();
    }

    @Test
    public void testGetFileNormalization() throws Exception {
        URI uri = new URI("http://example.com/");
        File file = dpm.deserialize(uri) == null ? new File(baseDir, "example.com" + File.separator + "index.json") : null;
        assertTrue(file != null && file.getPath().contains("index.json"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testSerializeFailsDueToUnreadableDirectoryStructure() throws Exception {
        File conflict = new File(baseDir, "readonly.com");
        assertTrue(conflict.createNewFile(), "Failed to create blocking file");
        URI uri = new URI("http://readonly.com/doc");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        IOException ex = assertThrows(IOException.class, () -> dpm.serialize(uri, doc));
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("directory") || ex.getMessage().toLowerCase().contains("not a directory"),
                "Expected directory-related error, got: " + ex.getMessage());
        conflict.delete();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testSerializeFailsDueToFileWhereDirectoryExpected() throws Exception {
        File conflictFile = new File(baseDir, "invalid.com");
        assertTrue(conflictFile.createNewFile(), "Failed to create conflict file");
        URI uri = new URI("http://invalid.com/doc");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        IOException ex = assertThrows(IOException.class, () -> dpm.serialize(uri, doc));
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("directory") || ex.getMessage().toLowerCase().contains("not a directory"),
                "Expected directory-related error, got: " + ex.getMessage());
        conflictFile.delete();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testDeleteMethod() throws Exception {
        URI uri = new URI("http://example.com/doc");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        dpm.serialize(uri, doc);
        assertTrue(dpm.delete(uri));
        assertFalse(dpm.delete(uri));
        File file = new File(baseDir, "example.com" + File.separator + "doc.json");
        file.setReadOnly();
        assertFalse(dpm.delete(uri));
        file.setWritable(true);
        file.delete();
    }

    @Test
    public void testDocumentPersistedToCorrectFilePath() throws IOException {
        URI uri = URI.create("http://example.com/apple/banana/cherry");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        dpm.serialize(uri, doc);
        File expectedFile = new File(baseDir,
                "example.com" + File.separator +
                        "apple" + File.separator +
                        "banana" + File.separator +
                        "cherry.json");

        assertTrue(expectedFile.exists(), "Document file should exist on disk at the correct location");
    }

    @Test
    public void testSerializeTriggersMkdirs() throws Exception {
        File parentDir = new File(baseDir, "newHost.com");
        URI uri = new URI("http://newHost.com/someDoc");
        Document doc = new DocumentImpl(uri, "text", Map.of("text", 1));
        if (parentDir.exists()) deleteDirectory(parentDir);
        dpm.serialize(uri, doc);
        File expectedFile = new File(parentDir, "someDoc.json");
        assertTrue(expectedFile.exists());
    }

    @Test
    public void testDeserializeTriggersMkdirs() throws Exception {
        URI uri = new URI("http://nonexistenthost.com/missing");
        File dir = new File(baseDir, "nonexistenthost.com");
        deleteDirectory(dir);
        Document doc = dpm.deserialize(uri);
        assertNull(doc);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testDeserializeOnMissingFileTriggersReturnNull() throws Exception {
        URI uri = new URI("http://ghost.com/phantom");
        File phantomFile = new File(baseDir, "ghost.com/phantom.json");
        if (phantomFile.exists()) phantomFile.delete();
        assertNull(dpm.deserialize(uri));
    }

    @Test
    public void testDeleteTriggersActualFileDelete() throws Exception {
        URI uri = new URI("http://delete.com/target");
        Document doc = new DocumentImpl(uri, "delete me", Map.of("delete", 1));
        dpm.serialize(uri, doc);
        File file = new File(baseDir, "delete.com/target.json");
        assertTrue(file.exists());
        assertTrue(dpm.delete(uri));
        assertFalse(file.exists());
    }

    @Test
    public void testBulkSerializeDeserializeDelete() throws Exception {
        int documentCount = 15;
        Map<URI, Document> originalDocs = new HashMap<>();
        for (int i = 1; i <= documentCount; i++) {
            URI uri = new URI("http://bulk.com/doc" + i);
            String text = "Text content " + i;
            Map<String, Integer> wordMap = Map.of("Text", 1, "content", 1, String.valueOf(i), 1);
            Document doc = new DocumentImpl(uri, text, wordMap);
            doc.setMetadataValue("index", String.valueOf(i));
            dpm.serialize(uri, doc);
            originalDocs.put(uri, doc);
            File expectedFile = new File(baseDir, "bulk.com" + File.separator + "doc" + i + ".json");
            assertTrue(expectedFile.exists(), "Serialized file for doc" + i + " should exist");
        }

        for (Map.Entry<URI, Document> entry : originalDocs.entrySet()) {
            Document deserialized = dpm.deserialize(entry.getKey());
            assertNotNull(deserialized, "Deserialized document should not be null for: " + entry.getKey());
            Document original = entry.getValue();
            assertEquals(original.getDocumentTxt(), deserialized.getDocumentTxt());
            assertEquals(original.getMetadataValue("index"), deserialized.getMetadataValue("index"));
        }
        for (URI uri : originalDocs.keySet()) {
            assertTrue(dpm.delete(uri), "Delete should return true for: " + uri);
            assertNull(dpm.deserialize(uri), "Document should no longer exist after delete: " + uri);
        }
    }
}