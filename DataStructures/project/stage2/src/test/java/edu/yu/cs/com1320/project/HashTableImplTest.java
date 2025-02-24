package edu.yu.cs.com1320.project;

import edu.yu.cs.com1320.project.impl.*;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class HashTableImplTest {
    @Test
    public void getAndPutTest() {
        //create table
        HashTable<String, String> hashT = new HashTableImpl<>();
        //get returns null when nothing is stored by key
        assertNull(hashT.get("nothing"));
        //NPE for get if key is null
        assertThrows(NullPointerException.class, () -> hashT.put(null, "bro"));
        //deleting nothing returns null
        assertNull(hashT.put("nothing", null));
        //put something original returns null
        assertNull(hashT.put("something", "or other"));
        //get key returns value
        assertEquals("or other", hashT.get("something"));
        //put something new returns old value
        assertEquals("or other", hashT.put("something", "new"));
        //deleting key returns old value
        assertEquals("new", hashT.put("something", null));
    }

    @Test
    public void containsKeyTest() {
        HashTable<Integer, String> hashT = new HashTableImpl<>();
        hashT.put(294, "words");
        hashT.put(301, "words");
        hashT.put(17, "words");
        //NPE for null key
        assertThrows(NullPointerException.class, () -> hashT.containsKey(null));
        //contains key
        assertTrue(hashT.containsKey(294));
        //doesn't
        assertFalse(hashT.containsKey(12));
    }

    @Test
    public void keySetTest() {
        HashTable<Integer, String> hashT = new HashTableImpl<>();
        hashT.put(294, "words");
        hashT.put(301, "words");
        hashT.put(17, "words");
        Set<Integer> keyChain = hashT.keySet();
        //key is in set
        assertTrue(keyChain.contains(294));
        //wrong key isn't in set
        assertFalse(keyChain.contains(12));
        //all three keys are in set
        assertEquals(3, keyChain.size());
    }

    @Test
    public void valuesAndSizeTest() {
        HashTable<Integer, String> hashT = new HashTableImpl<>();
        hashT.put(294, "words");
        hashT.put(301, "mean");
        hashT.put(17, "something");
        hashT.put(12, "I think");
        Collection<String> words = hashT.values();
        //value is in collection
        assertTrue(words.contains("mean"));
        //wrong value isn't in collection
        assertFalse(words.contains("nothing"));
        //all four values are in collection
        assertEquals(4, words.size());
        //size is 4
        assertEquals(4, hashT.size());
    }
}
