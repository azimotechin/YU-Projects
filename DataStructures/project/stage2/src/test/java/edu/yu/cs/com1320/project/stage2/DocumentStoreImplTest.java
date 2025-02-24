package edu.yu.cs.com1320.project.stage2;

import edu.yu.cs.com1320.project.stage2.impl.DocumentStoreImpl;
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
}
