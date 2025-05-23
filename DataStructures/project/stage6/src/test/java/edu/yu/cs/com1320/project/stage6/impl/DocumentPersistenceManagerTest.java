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
        wordMap.put("Hello", 1); // capital H
        wordMap.put("hello", 1); // lowercase h
        wordMap.put("world", 1);
        wordMap.put("test", 1);

        Document originalDoc = new DocumentImpl(uri, text, wordMap);
        originalDoc.setMetadataValue("just so cool", "super cool");
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertEquals(originalDoc.getDocumentTxt(), deserializedDoc.getDocumentTxt());
        assertEquals(originalDoc.getMetadataValue("author"), deserializedDoc.getMetadataValue("author"));
        assertEquals(1, deserializedDoc.wordCount("Hello")); // capital H
        assertEquals(1, deserializedDoc.wordCount("hello")); // lowercase h
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
        assertNull(deserializedDoc);  // Should return null if the file does not exist
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
        byte[] largeData = new byte[100000];  // Simulate a large binary file
        Document originalDoc = new DocumentImpl(uri, largeData);
        dpm.serialize(uri, originalDoc);
        Document deserializedDoc = dpm.deserialize(uri);
        assertNotNull(deserializedDoc);
        assertArrayEquals(largeData, deserializedDoc.getDocumentBinaryData());
    }

    @Test
    public void testSerializeAndDeserializeClasspathUri() throws Exception {
        URI uri = new URI("classpath:/some/resource");
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

}