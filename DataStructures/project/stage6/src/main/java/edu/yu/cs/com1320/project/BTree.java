package edu.yu.cs.com1320.project;

import edu.yu.cs.com1320.project.stage6.PersistenceManager;

import java.io.IOException;

public interface BTree<Key extends Comparable<Key>, Value> {
    Value get(Key k);
    Value put(Key k, Value v);
    void moveToDisk(Key k) throws IOException;
    void setPersistenceManager(PersistenceManager<Key,Value> pm);
}