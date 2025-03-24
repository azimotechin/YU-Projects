package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.Trie;

import java.util.*;

public class TrieImpl<Value> implements Trie<Value> {
    // variables
    private static final int alphabetSize = 128;
    private Node<Value> root;

    // node class
    private static class Node<Value> {
        final Node<Value>[] links;
        final Set<Value> values;
        @SuppressWarnings("unchecked")
        public Node() {
            links = new Node[alphabetSize];
            values = new HashSet<>();
        }
    }

    // constructor
    public TrieImpl() {
        this.root = new Node<>();
    }

    // setters
    @Override
    public void put(String key, Value val) {
        if (val == null) {
            this.deleteAll(key);
        }
        else {
            this.root = put(this.root, key, val, 0);
        }
    }

    private Node<Value> put(Node<Value> x, String key, Value val, int d) {
        if (x == null) {
            x = new Node<>();
        }
        if (d == key.length()) {
            x.values.add(val);
            return x;
        }
        char c = key.charAt(d);
        x.links[c] = this.put(x.links[c], key, val, d + 1);
        return x;
    }

    // getters
    @Override
    public List<Value> getSorted(String key, Comparator<Value> comparator) {
        Node<Value> x = this.get(this.root, key, 0);
        if (x == null)
        {
            return null;
        }
        List<Value> sorted = new ArrayList<>(x.values);
        sorted.sort(comparator);
        return sorted;
    }

    @Override
    public Set<Value> get(String key) {
        Node<Value> node = get(this.root, key, 0);
        if (node != null) {
            return node.values;
        }
        return Set.of();
    }

    private Node<Value> get(Node<Value> x, String key, int d) {
        if (x == null) {
            return null;
        }
        if (d == key.length()) {
            return x;
        }
        char c = key.charAt(d);
        return this.get(x.links[c], key, d + 1);
    }

    @Override
    public List<Value> getAllWithPrefixSorted(String prefix, Comparator<Value> comparator) {
        Node<Value> n = get(this.root, prefix, 0);
        if (n != null) {
            List<Value> sorted = new ArrayList<>(getAllWithPrefix(n));
            sorted.sort(comparator);
            return sorted;
        }
        return List.of();
    }

    private Set<Value> getAllWithPrefix(Node<Value> x) {
        Set<Value> allValues = new HashSet<>(x.values);
        for (Node<Value> n : x.links) {
            if (n != null) {
                allValues.addAll(getAllWithPrefix(n));
            }
        }
        return allValues;
    }

    @Override
    public Set<Value> deleteAllWithPrefix(String prefix) {
        Set<Value> deleted = new HashSet<>(getAllWithPrefix(get(this.root, prefix, 0)));
        this.root = deleteAllWithPrefix(this.root, prefix, 0);
        return deleted;
    }

    private Node<Value> deleteAllWithPrefix(Node<Value> x, String prefix, int d) {
        if (x == null) {
            return null;
        }
        if (d == prefix.length()) {
            return null;
        } else {
            char c = prefix.charAt(d);
            x.links[c] = this.deleteAllWithPrefix(x.links[c], prefix, d + 1);
        }
        if (!x.values.isEmpty()) {
            return x;
        }
        for (Node<Value> n : x.links) {
            if (n != null) {
                return x;
            }
        }
        return null;
    }

    @Override
    public Set<Value> deleteAll(String key) {
        Set<Value> deleted = new HashSet<>(get(key));
        this.root = deleteAll(this.root, key, 0);
        return deleted;
    }

    private Node<Value> deleteAll(Node<Value> x, String key, int d) {
        if (x == null) {
            return null;
        }
        if (d == key.length()) {
            x.values.clear();
        } else {
            char c = key.charAt(d);
            x.links[c] = this.deleteAll(x.links[c], key, d + 1);
        }
        if (!x.values.isEmpty()) {
            return x;
        }
        for (Node<Value> n : x.links) {
            if (n != null) {
                return x;
            }
        }
        return null;
    }

    @Override
    public Value delete(String key, Value val) {
        if (!get(key).contains(val)) {
            return null;
        }
        this.root = delete(this.root, key, val, 0);
        return val;
    }

    private Node<Value> delete(Node<Value> x, String key, Value val, int d) {
        if (x == null) {
            return null;
        }
        if (d == key.length()) {
            x.values.remove(val);
        } else {
            char c = key.charAt(d);
            x.links[c] = this.delete(x.links[c], key, val, d + 1);
        }
        if (!x.values.isEmpty()) {
            return x;
        }
        for (Node<Value> n : x.links) {
            if (n != null) {
                return x;
            }
        }
        return null;
    }

}