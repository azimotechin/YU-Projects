package edu.yu.cs.com1320.project;

import java.util.Collection;
import java.util.Set;

/**
 * Instances of HashTable should be constructed with two type parameters, one for the type of the keys in the table and one for the type of the values
 *
 * @param <Key>
 * @param <Value>
 */
public interface HashTable<Key, Value> {
    /**
     * @param k the key whose value should be returned
     * @return the value that is stored in the HashTable for k, or null if there is no such key in the table
     */
    Value get(Key k);

    /**
     * @param k the key at which to store the value
     * @param v the value to store
     *          To delete an entry, put a null value.
     * @return if the key was already present in the HashTable, return the previous value stored for the key. If the key was not already present, return null.
     */
    Value put(Key k, Value v);

    /**
     * @param key the key whose presence in the hashtabe we are inquiring about
     * @return true if the given key is present in the hashtable as a key, false if not
     * @throws NullPointerException if the specified key is null
     */
    boolean containsKey(Key key);

    /**
     * @return an unmodifiable set of all the keys in this HashTable
     * @see java.util.Collections#unmodifiableSet(Set)
     */
    Set<Key> keySet();

    /**
     * @return an unmodifiable collection of all the values in this HashTable
     * @see java.util.Collections#unmodifiableCollection(Collection)
     */
    Collection<Value> values();

    /**
     * @return how entries there currently are in the HashTable
     */
    int size();
}