package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.Trie;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class TrieImpl<Value> implements Trie<Value> {
    @Override
    public void put(String key, Value val) {

    }

    @Override
    public List<Value> getSorted(String key, Comparator<Value> comparator) {
        return List.of();
    }

    @Override
    public Set<Value> get(String key) {
        return Set.of();
    }

    @Override
    public List<Value> getAllWithPrefixSorted(String prefix, Comparator<Value> comparator) {
        return List.of();
    }

    @Override
    public Set<Value> deleteAllWithPrefix(String prefix) {
        return Set.of();
    }

    @Override
    public Set<Value> deleteAll(String key) {
        return Set.of();
    }

    @Override
    public Value delete(String key, Value val) {
        return null;
    }
}
