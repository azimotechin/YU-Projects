package edu.yu.cs.com1320.project;

import edu.yu.cs.com1320.project.impl.TrieImpl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.*;

public class TrieImplTest {
    // put test
    @Test
    public void testPutAndGet() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        Set<Integer> intSet = intTrie.get("words");
        Set<Integer> badIntSet = intTrie.get("wds");
        assertTrue(badIntSet.isEmpty());
        assertTrue(intSet.contains(5));
        assertTrue(intSet.contains(1));
        assertTrue(intSet.contains(48));
        assertTrue(intSet.contains(57));
        assertFalse(intSet.contains(44));
        assertFalse(intSet.contains(59));
        intTrie.put("words", null);
        Set<Integer> empty = intTrie.get("words");
        assertTrue(empty.isEmpty());
    }

    // get sorted test
    @Test
    public void testGetSorted() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        List<Integer> intList = intTrie.getSorted("words", Comparator.reverseOrder());
        assertTrue(intList.contains(5));
        assertTrue(intList.contains(1));
        assertTrue(intList.contains(48));
        assertTrue(intList.contains(57));
        assertFalse(intList.contains(44));
        assertFalse(intList.contains(59));
        assertEquals(57, intList.get(0));
        assertEquals(48, intList.get(1));
        assertEquals(5, intList.get(2));
        assertEquals(1, intList.get(3));
    }

    // get all w/ prefix sorted test
    @Test
    public void testGetAllWithPrefixSorted() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        List<Integer> intList = intTrie.getAllWithPrefixSorted("wor", Comparator.reverseOrder());
        assertTrue(intList.contains(5));
        assertTrue(intList.contains(1));
        assertTrue(intList.contains(48));
        assertTrue(intList.contains(57));
        assertFalse(intList.contains(44));
        assertTrue(intList.contains(59));
        assertEquals(59, intList.get(0));
        assertEquals(57, intList.get(1));
        assertEquals(48, intList.get(2));
        assertEquals(5, intList.get(3));
        assertEquals(1, intList.get(4));
    }

    // delete all with prefix test
    @Test
    public void testDeleteAllWithPrefix() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        Set<Integer> intSet = intTrie.deleteAllWithPrefix("wor");
        assertTrue(intSet.contains(5));
        assertTrue(intSet.contains(1));
        assertTrue(intSet.contains(48));
        assertTrue(intSet.contains(57));
        assertFalse(intSet.contains(44));
        assertTrue(intSet.contains(59));
    }

    // delete all test
    @Test
    public void testDeleteAll() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        Set<Integer> intSet = intTrie.deleteAll("word");
        assertFalse(intSet.contains(5));
        assertFalse(intSet.contains(1));
        assertFalse(intSet.contains(48));
        assertFalse(intSet.contains(57));
        assertFalse(intSet.contains(44));
        assertFalse(intSet.isEmpty());
        assertTrue(intSet.contains(59));
    }

    // delete test
    @Test
    public void testDelete() {
        Trie<Integer> intTrie = new TrieImpl<>();
        intTrie.put("words", 5);
        intTrie.put("words", 1);
        intTrie.put("wods", 44);
        intTrie.put("words", 48);
        intTrie.put("words", 57);
        intTrie.put("word", 59);
        int i = intTrie.delete("words", 48);
        Set<Integer> intSet = intTrie.get("words");
        assertNull(intTrie.delete("words", 44));
        assertTrue(intSet.contains(5));
        assertTrue(intSet.contains(1));
        assertFalse(intSet.contains(48));
        assertTrue(intSet.contains(57));
        assertFalse(intSet.contains(44));
        assertFalse(intSet.contains(59));
        assertEquals(48, i);
    }
}
