package edu.yu.cs.com1320.project.stage5.impl;

import edu.yu.cs.com1320.project.stage5.Document;
import edu.yu.cs.com1320.project.stage5.DocumentStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentStoreImplTest {
    private DocumentStore store;
    private URI uri1;
    private URI uri2;
    private URI uri3;
    private URI uri4;
    private URI uri5;
    private URI uri6;
    private URI uri7;
    private URI uri8;
    private URI uri9;
    private URI uri10;
    private URI uri11;
    private String text1;
    private String text2;
    private String text3;
    private String text4;
    private String text5;
    private String text6;
    private String text7;
    private String text8;
    private String text9;
    private String text10;
    private String text11;

    @BeforeEach
    void setUp() throws URISyntaxException {
        store = new DocumentStoreImpl();
        uri1 = new URI("http://doc1.com");
        uri2 = new URI("http://doc2.com");
        uri3 = new URI("http://doc3.com");
        uri4 = new URI("http://doc4.com");
        uri5 = new URI("http://doc5.com");
        uri6 = new URI("http://doc6.com");
        uri7 = new URI("http://doc7.com");
        uri8 = new URI("http://doc8.com");
        uri9 = new URI("http://doc9.com");
        uri10 = new URI("http://doc10.com");
        uri11 = new URI("http://doc11.com");
        text1 = "Run, run, run until the sun sets and the world turns quiet. " +
                "She laughed and laughed, louder each time, until the room echoed with joy. " +
                "It was cold, so cold, colder than he ever remembered. " +
                "The night was dark—dark with secrets, dark with silence, dark with fear. " +
                "Again and again and again, he tried, hoping this time would be different.";
        text2 = "The the dog dog barked barked again again. " +
                "It it ran ran through through the the yard yard. " +
                "Neighbors neighbors watched watched from from windows windows. " +
                "Leaves leaves blew blew across across the the lawn lawn. " +
                "Finally finally it it stopped stopped barking barking.";
        text3 = "She she opened opened the the door door slowly slowly. " +
                "The the cold cold air air rushed rushed in in. " +
                "Lights lights flickered flickered above above. " +
                "Her her hands hands shook shook gently gently. " +
                "Still still she she stepped stepped inside inside.";
        text4 = "Rain rain tapped tapped on on the the glass glass. " +
                "Thunder thunder crashed crashed in in the the distance distance. " +
                "She she pulled pulled the the blanket blanket tight tight. " +
                "Books books lay lay scattered scattered around around. " +
                "Sleep sleep would would not not come come.";
        text5 = "He he ran ran down down the the hall hall. " +
                "Footsteps footsteps echoed echoed behind behind him him. " +
                "He he didn’t didn’t stop stop moving moving. " +
                "Breath breath came came hard hard and and fast fast. " +
                "He he reached reached the the door door finally finally.";
        text6 = "The the music music played played louder louder. " +
                "Colors colors danced danced on on the the walls walls. " +
                "She she laughed laughed without without reason reason. " +
                "Voices voices rose rose then then faded faded. " +
                "Time time slowed slowed and and stopped stopped.";
        text7 = "Birds birds flew flew across across the the sky sky. " +
                "Waves waves crashed crashed onto onto the the shore shore. " +
                "The the sun sun shimmered shimmered above above. " +
                "Footprints footprints marked marked the the sand sand. " +
                "Peace peace settled settled into into her her chest chest.";
        text8 = "Steam steam rose rose from from the the mug mug. " +
                "He he sipped sipped slowly slowly and and stared stared. " +
                "Thoughts thoughts circled circled in in his his mind mind. " +
                "Words words were were hard hard to to form form. " +
                "Quiet quiet was was better better today today.";
        text9 = "Lights lights flashed flashed red red and and blue blue. " +
                "Sirens sirens blared blared into into the the night night. " +
                "Crowds crowds gathered gathered in in silence silence. " +
                "Tension tension clung clung to to the the air air. " +
                "No one one spoke spoke at at all all.";
        text10 = "Papers papers flew flew across across the the room room. " +
                "Wind wind burst burst through through the the open open window window. " +
                "He he chased chased after after them them. " +
                "Fingers fingers grasped grasped one one by by one one. " +
                "He he sighed sighed when when finished finished.";
        text11 = "The the candle candle flickered flickered dimly dimly. " +
                "Shadows shadows danced danced along along the the walls walls. " +
                "She she whispered whispered to to no no one one. " +
                "Memories memories came came rushing rushing back back. " +
                "Then then silence silence filled filled the the room room.";
    }

    private InputStream stringToStream(String text) {
        return new ByteArrayInputStream(text.getBytes());
    }

    @Test
    void testGet() throws IOException {
        store.put(stringToStream(text1), uri1, DocumentStore.DocumentFormat.TXT);
        store.put(stringToStream(text2), uri2, DocumentStore.DocumentFormat.BINARY);
        store.put(stringToStream(text3), uri3, DocumentStore.DocumentFormat.TXT);
        store.put(stringToStream(text4), uri4, DocumentStore.DocumentFormat.BINARY);
        store.put(stringToStream(text5), uri5, DocumentStore.DocumentFormat.TXT);
        store.setMaxDocumentCount(4);
        assertNull(store.get(uri1));
        store.setMaxDocumentBytes(800);
        assertNull(store.get(uri2));
    }

    @Test
    void testPut() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> store.put(stringToStream(text1), null, DocumentStore.DocumentFormat.TXT));
        assertThrows(IllegalArgumentException.class, () -> store.put(stringToStream(text1), URI.create(""), DocumentStore.DocumentFormat.TXT));
        assertThrows(IllegalArgumentException.class, () -> store.put(stringToStream(text1), uri1, null));
        assertEquals(0, store.put(stringToStream(text1), uri1, DocumentStore.DocumentFormat.TXT));
        List<Document> list = store.search("run");
        assertFalse(list.isEmpty());
        int oldDocHashCode = store.get(uri1).hashCode();
        assertEquals(oldDocHashCode, store.put(stringToStream(text2), uri1, DocumentStore.DocumentFormat.TXT));
        list = store.search("run");
        assertTrue(list.isEmpty());
        assertNotNull(store.get(uri1));
        assertEquals(0, store.put(stringToStream(text3), uri2, DocumentStore.DocumentFormat.TXT));
        store.setMaxDocumentCount(1);
        assertNull(store.get(uri1));
        assertNull(store.get(uri7));
        assertEquals(0, store.put(stringToStream(text4), uri1, DocumentStore.DocumentFormat.TXT));
        assertNull(store.get(uri1));
    }
}
