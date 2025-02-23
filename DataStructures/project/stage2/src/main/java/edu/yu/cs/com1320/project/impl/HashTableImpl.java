package edu.yu.cs.com1320.project.impl;
import edu.yu.cs.com1320.project.HashTable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class HashTableImpl<Key, Value> implements HashTable<Key, Value> {
    // entry class
    private static class Entry<Key, Value> {
        Key key;
        Value value;
        // entry constructor
        Entry<Key, Value> next = null;
        public Entry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }
    }
    // variable
    private final Entry<Key, Value>[] table;
    // constructor
    @SuppressWarnings("unchecked")
    public HashTableImpl() {
        this.table = new Entry[5];
    }
    // getter
    @Override
    public Value get(Key key) {
        Entry<Key, Value> current = this.table[hash(key)];
        while (current != null) {
            if (current.key.equals(key)) {
                return current.value;
            }
            current = current.next;
        }
        return null;
    }
    // setter and deleter
    @Override
    public Value put(Key k, Value v) {
        if (k == null) {
            throw new NullPointerException("key is null");
        }
        Entry<Key, Value> current = this.table[hash(k)];
        Entry<Key, Value> previous = null;
        if (v == null) {
            while (current != null) {
                if (current.key.equals(k)) {
                    if (previous == null) {
                        this.table[hash(k)] = current.next;
                    } else {
                        previous.next = current.next;
                    }
                    return current.value;
                }
                previous = current;
                current = current.next;
            }
            return null;
        }
        current = this.table[hash(k)];
        while (current != null) {
            if (current.key.equals(k)) {
                Value oldValue = current.value;
                current.value = v;
                return oldValue;
            }
            current = current.next;
        }
        Entry<Key, Value> newEntry = new Entry<>(k, v);
        newEntry.next = this.table[hash(k)];
        this.table[hash(k)] = newEntry;
        return null;
    }
    // contains
    @Override
    public boolean containsKey(Key key) {
        if (key == null) {
            throw new NullPointerException("key is null");
        }
        Entry<Key, Value> current = this.table[hash(key)];
        while (current != null) {
            if (current.key.equals(key)) {
                return true;
            }
            current = current.next;
        }
        return false;
    }
    // set of keys
    @Override
    public Set<Key> keySet() {
        Set<Key> keys = new java.util.HashSet<>();
        for (Entry<Key, Value> current : this.table) {
            while (current != null) {
                keys.add(current.key);
                current = current.next;
            }
        }
        return java.util.Collections.unmodifiableSet(keys);
    }
    // collection of values
    @Override
    public Collection<Value> values() {
        List<Value> values = new java.util.ArrayList<>();
        for (Entry<Key, Value> current : this.table) {
            while (current != null) {
                values.add(current.value);
                current = current.next;
            }
        }
        return java.util.Collections.unmodifiableCollection(values);
    }
    // number of docs in table
    @Override
    public int size() {
        int count = 0;
        for (Entry<Key, Value> current : this.table) {
            while (current != null) {
                count++;
                current = current.next;
            }
        }
        return count;
    }
    // hash
    private int hash(Key key) {
        return (key.hashCode() < 0 ? -key.hashCode() : key.hashCode()) % 5;
    }
}
