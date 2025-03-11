package edu.yu.cs.com1320.project;

import edu.yu.cs.com1320.project.impl.TrieImpl;

import static org.junit.jupiter.api.Assertions.*;

import edu.yu.cs.com1320.project.stage4.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrieImplTest {
    // trie construction
    @BeforeEach void createTrie() {
        Trie<Document> docTrie = new TrieImpl<>();
    }

    // put test
    @Test
    public void testPut() {}

    // get sorted test
    @Test
    public void testGetSorted() {}

    // get all w/ prefix sorted test
    @Test
    public void testGetAllWithPrefixSorted() {}

    // delete all with prefix test
    @Test
    public void testDeleteAllWithPrefix() {}

    // delete all test
    @Test
    public void testDeleteAll() {}

    // delete test
    @Test
    public void testDelete() {}
}
