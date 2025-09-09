package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.MinHeap;

import java.util.Arrays;
import java.util.NoSuchElementException;

public class MinHeapImpl<E extends Comparable<E>> extends MinHeap<E> {
    @SuppressWarnings("unchecked")
    public MinHeapImpl() {
        this.elements = (E[]) new Comparable[11];
    }

    @Override
    public void reHeapify(E element) {
        int index = getArrayIndex(element);
        upHeap(index);
        downHeap(index);
    }

    @Override
    protected int getArrayIndex(E element) {
        for (int i = 1; i <= count; i++) {
            if (this.elements[i].equals(element)) {
                return i;
            }
        }
        throw new NoSuchElementException("Element not found in heap");
    }

    @Override
    protected void doubleArraySize() {
        this.elements = Arrays.copyOf(this.elements, this.elements.length * 2);
    }
}