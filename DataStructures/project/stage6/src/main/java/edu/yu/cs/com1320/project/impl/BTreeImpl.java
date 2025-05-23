package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.BTree;
import edu.yu.cs.com1320.project.stage6.PersistenceManager;

import java.io.IOException;

public class BTreeImpl<Key extends Comparable<Key>, Value> implements BTree<Key, Value> {
    private final int MAX = 6; // Max number of children per node
    private Node root;
    private int height;
    @SuppressWarnings("rawtypes")
    private PersistenceManager pm;

    private class Node {
        private int entryCount; // Number of entries in the node
        private final Entry[] entries;

        @SuppressWarnings({"unchecked"})
        public Node() {
            this.entryCount = 0;
            entries = new BTreeImpl.Entry[MAX];
        }
    }

    private class Entry {
        private final Key key;
        private Value val;
        private final Node child;

        public Entry(Key key, Value val, Node child) {
            this.key = key;
            this.val = val;
            this.child = child;
        }
    }

    public BTreeImpl() {
        this.root = new Node();
        this.height = 0;
        this.pm = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value get(Key key) {
        Entry e = this.get(this.root, key, this.height);
        if (e != null) {
            if (e.val == null && this.pm != null) {
                try {
                    e.val = (Value) pm.deserialize(key);
                    pm.delete(key);
                } catch (IOException ex) {
                    throw new RuntimeException("Cannot deserialize");
                }
            }
            return e.val;
        }
        return null;
    }

    private Entry get(Node currentNode, Key key, int height) {
        Entry[] entries = currentNode.entries;
        if (height == 0) {
            for (int j = 0; j < currentNode.entryCount; j++) {
                if (isEqual(key, entries[j].key)) {
                    return entries[j];
                }
            }
            return null;
        } else {
            for (int j = 0; j < currentNode.entryCount; j++) {
                if (j + 1 == currentNode.entryCount || less(key, entries[j + 1].key)) {
                    return this.get(entries[j].child, key, height - 1);
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value put(Key k, Value v) {
        if (k == null) {
            throw new IllegalArgumentException("key is null");
        }
        Entry keyAlreadyThere = this.get(this.root, k, this.height);
        if (keyAlreadyThere != null) {
            try {
                this.pm.delete(k);
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete persisted value for key: " + k, e);
            }
            Value temp = keyAlreadyThere.val;
            keyAlreadyThere.val = v;
            return temp;
        }
        Node newNode = this.put(this.root, k, v, this.height);
        if (newNode == null) {
            return null;
        }
        Node newRoot = new Node();
        newRoot.entries[0] = new Entry(this.root.entries[0].key, null, this.root);
        newRoot.entries[1] = new Entry(newNode.entries[0].key, null, newNode);
        this.root = newRoot;
        newRoot.entryCount = 2;
        this.height++;
        return null;
    }

    private Node put(Node currentNode, Key key, Value val, int height) {
        int j;
        Entry newEntry = null;
        if (height == 0) {
            newEntry = new Entry(key, val, null);
            for (j = 0; j < currentNode.entryCount; j++) {
                if (less(key, currentNode.entries[j].key)) {
                    break;
                }
            }
        } else {
            for (j = 0; j < currentNode.entryCount; j++) {
                if ((j + 1 == currentNode.entryCount) || less(key, currentNode.entries[j + 1] != null ? currentNode.entries[j + 1].key : null)) {
                    if (currentNode.entries[j] != null) {
                        Node newNode = this.put(currentNode.entries[j++].child, key, val, height - 1);
                        if (newNode == null) {
                            return null;
                        }
                        newEntry = new Entry(newNode.entries[0].key, null, newNode);
                        break;
                    }
                }
            }
        }
        for (int i = currentNode.entryCount; i > j; i--) {
            currentNode.entries[i] = currentNode.entries[i - 1];
        }
        currentNode.entries[j] = newEntry;
        currentNode.entryCount++;
        if (currentNode.entryCount < MAX) {
            return null;
        } else {
            return this.split(currentNode);
        }
    }

    private Node split(Node currentNode) {
        Node newNode = new Node();
        for (int j = 0; j < MAX / 2; j++) {
            newNode.entries[j] = currentNode.entries[MAX / 2 + j];
            currentNode.entries[MAX / 2 + j] = null;
        }
        currentNode.entryCount = MAX / 2;
        newNode.entryCount = MAX/2;
        return newNode;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void moveToDisk(Key k) throws IOException {
        if (k == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (get(k) == null) {
            return;
        }
        Value v = get(k);
        pm.serialize(k, v);
        put(this.root, k, null, this.height);
    }

    @Override
    public void setPersistenceManager(PersistenceManager<Key, Value> pm) {
        this.pm = pm;
    }

    private boolean less(Key k1, Key k2) {
        return k1.compareTo(k2) < 0;
    }

    private boolean isEqual(Key k1, Key k2) {
        return k1.compareTo(k2) == 0;
    }
}
