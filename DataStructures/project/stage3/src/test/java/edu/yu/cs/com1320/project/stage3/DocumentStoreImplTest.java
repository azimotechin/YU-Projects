package edu.yu.cs.com1320.project.stage3;

import edu.yu.cs.com1320.project.stage3.impl.DocumentStoreImpl;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentStoreImplTest {
    /* things we're testing:
    * constructor
    * get and delete
    * put
    * get/set metadata
     */
    @Test
    public void testConstructorPutIntValueGetAndFormat() throws URISyntaxException, IOException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com");
        InputStream input = new ByteArrayInputStream("bro".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.TXT);
        assertEquals("bro", store.get(uri).getDocumentTxt());
        URI uri2 = new URI("http://dude.com");
        InputStream input2 = new ByteArrayInputStream("dude".getBytes());
        store.put(input2, uri2, DocumentStore.DocumentFormat.BINARY);
        assertEquals("dude", new String(store.get(uri2).getDocumentBinaryData(), StandardCharsets.UTF_8));
        assertTrue(store.put(input, uri, DocumentStore.DocumentFormat.TXT) != store.put(input2, uri2, DocumentStore.DocumentFormat.BINARY));
    }
    @Test
    public void testPutWithIAException() throws URISyntaxException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com");
        URI emptyURI = new URI("");
        InputStream input = new ByteArrayInputStream("bro".getBytes());
        assertThrows(IllegalArgumentException.class, () -> store.put(input, null, DocumentStore.DocumentFormat.TXT));
        assertThrows(IllegalArgumentException.class, () -> store.put(input, emptyURI, DocumentStore.DocumentFormat.TXT));
        assertThrows(IllegalArgumentException.class, () -> store.put(input, uri, null));
    }
    @Test
    public void testPutWithIOException() throws URISyntaxException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com/test");
        InputStream input = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Simulated IO Exception");
            }
        };
        assertThrows(IOException.class, () -> store.put(input, uri, DocumentStore.DocumentFormat.TXT));
    }
    @Test
    public void testPutDelete() throws URISyntaxException, IOException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com");
        InputStream input = new ByteArrayInputStream("bro".getBytes());
        assertFalse(store.delete(uri));
        assertEquals(0, store.put(input, uri, DocumentStore.DocumentFormat.TXT));
        assertTrue(store.put(null, uri, DocumentStore.DocumentFormat.TXT) != 0);
        assertEquals(0, store.put(input, uri, DocumentStore.DocumentFormat.TXT));
    }
    @Test
    public void testSetAndGetMetadata() throws IOException, URISyntaxException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com");
        InputStream input = new ByteArrayInputStream("everyone".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.TXT);
        assertNull(store.setMetadata(uri, "bro", "valuable"));
        assertEquals("valuable", store.getMetadata(uri, "bro"));
        assertThrows(IllegalArgumentException.class, () -> store.setMetadata(null, "bro", "value"));
        URI empty = new URI("");
        assertThrows(IllegalArgumentException.class, () -> store.getMetadata(empty, "bro"));
        URI uri2 = new URI("http://dude.com");
        assertThrows(IllegalArgumentException.class, () -> store.getMetadata(uri2, "bro"));
    }

    @Test
    public void testUndo() throws URISyntaxException, IOException {
        DocumentStore store = new DocumentStoreImpl();
        URI uri = new URI("http://bro.com");
        InputStream input = new ByteArrayInputStream("everyone".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.TXT);
        assertEquals("everyone", store.get(uri).getDocumentTxt());
        store.undo();
        input = new ByteArrayInputStream("everyone".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.TXT);
        assertEquals("everyone", store.get(uri).getDocumentTxt());
        input = new ByteArrayInputStream("guys".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.BINARY);
        assertEquals("guys", new String(store.get(uri).getDocumentBinaryData(), StandardCharsets.UTF_8));
        store.undo();
        assertEquals("everyone", store.get(uri).getDocumentTxt());
        input = new ByteArrayInputStream("bros".getBytes());
        store.put(input, uri, DocumentStore.DocumentFormat.TXT);
        assertEquals("bros", store.get(uri).getDocumentTxt());
        URI uri2 = new URI("http://exp.com");
        input = new ByteArrayInputStream("guys".getBytes());
        store.put(input, uri2, DocumentStore.DocumentFormat.TXT);
        assertEquals("guys", store.get(uri2).getDocumentTxt());
        store.undo(uri);
        assertEquals("everyone", store.get(uri).getDocumentTxt());
        store.delete(uri);
        assertNull(store.get(uri));
        store.undo();
        assertEquals("everyone", store.get(uri).getDocumentTxt());
        store.setMetadata(uri, "123", "456");
        store.setMetadata(uri, "123", null);
        store.setMetadata(uri2, "142", "12");
        store.setMetadata(uri, "bro", "intel");
        assertNull(store.getMetadata(uri, "123"));
        store.undo();
        store.undo(uri);
        assertEquals("456", store.getMetadata(uri, "123"));
        assertEquals("12", store.getMetadata(uri2, "142"));
    }


    /* @Test
    public void testUndo2() throws URISyntaxException, IOException {
        DocumentStore store = new DocumentStoreImpl();
        InputStream input = new ByteArrayInputStream("everyone".getBytes());
        for (int i = 0; i < 20; i++) {
            store.put(input, URI.create("http://bro" + i + ".com"), DocumentStore.DocumentFormat.TXT);
        }
        for (int i = 0; i < 20; i++) {
            store.setMetadata(URI.create("http://bro" + i + ".com"), "bro", "yo");
        }
        store.undo(URI.create("http://bro4.com"));
        assertNull(store.getMetadata(URI.create("http://bro4.com"), "bro"));
        assertEquals("yo", store.getMetadata(URI.create("http://bro7.com"), "bro"));
    } */
}
