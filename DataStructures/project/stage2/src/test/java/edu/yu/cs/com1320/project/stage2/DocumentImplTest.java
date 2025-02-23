package edu.yu.cs.com1320.project.stage2;
import edu.yu.cs.com1320.project.HashTable;
import edu.yu.cs.com1320.project.impl.DocumentImpl;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.URISyntaxException;


public class DocumentImplTest {
    /* Things to test:
    * doc was created (print lines)
    *   each constructor works
    * .hashcode and .equals work
    * getKey()
    * get text and b-data
    * get metadata and m-value
    * set metadata
    */
    @Test
    public void testTxtConstructorGetKeyGetDocumentTxt() throws URISyntaxException {
        URI testURI = new URI("http://bro.com");
        Document txt = new DocumentImpl(testURI, "testText");
        assertEquals(txt.getKey(), testURI);
        assertEquals("testText", txt.getDocumentTxt());
        assertNull(txt.getDocumentBinaryData());
    }
    @Test
    public void testBDataConstructorGetDocumentBinaryData() throws URISyntaxException{
        URI testURI = new URI("http://bro.com");
        byte[] testBData = new byte[]{1, 2, 3, 4};
        Document bData = new DocumentImpl(testURI, testBData);
        assertEquals(bData.getKey(), testURI);
        assertEquals(bData.getDocumentBinaryData(), testBData);
        assertNull(bData.getDocumentTxt());
    }
    @Test
    public void testHashCodeEquals() throws URISyntaxException {
        URI testURI = new URI("http://bro.com");
        Document txt = new DocumentImpl(testURI, "testText");
        Document other = txt;
        URI testURI2 = new URI("http://dude.com");
        Document txt2 = new DocumentImpl(testURI2, "testText");
        assertTrue(txt.equals(other));
        assertTrue(txt.hashCode() != txt2.hashCode());
    }
    @Test
    public void testSetAndGetMetadataValue() throws URISyntaxException {
        URI testURI = new URI("http://bro.com");
        Document txt = new DocumentImpl(testURI, "testText");
        assertThrows(IllegalArgumentException.class, () -> txt.setMetadataValue("", "bro"));
        assertThrows(IllegalArgumentException.class, () -> txt.setMetadataValue(null, "bro"));
        assertNull(txt.getMetadataValue("bro"));
        txt.setMetadataValue("bro", "12345");
        assertEquals("12345", txt.getMetadataValue("bro"));
        assertEquals("12345", txt.setMetadataValue("bro", "54321"));
        txt.setMetadataValue("dude", "46");
        HashTable<String, String> copy = txt.getMetadata();
        assertEquals("54321", copy.get("bro"));
        assertEquals("46", copy.get("dude"));
    }
}
