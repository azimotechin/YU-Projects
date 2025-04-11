package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.stage5.Document;
import edu.yu.cs.com1320.project.stage5.impl.DocumentImpl;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class MinHeapImplTest {

    @Test
    void testReHeapify() throws Exception {
        MinHeapImpl<Document> heap = new MinHeapImpl<>();
        Document d1 = new DocumentImpl(new URI("doc://1"), "Doc One");
        Document d2 = new DocumentImpl(new URI("doc://2"), "Doc Two");
        Document d3 = new DocumentImpl(new URI("doc://3"), "Doc Three");
        heap.insert(d1);
        heap.insert(d2);
        heap.insert(d3);
        assertEquals(d1, heap.remove());
        assertEquals(d2, heap.remove());
        heap.insert(d1);
        heap.insert(d2);
        heap.insert(d3);
        d3.setLastUseTime(50);
        heap.reHeapify(d3);
        assertEquals(d3, heap.remove());
    }

    @Test
    public void testDoubleArraySize() {
        MinHeapImpl<Integer> heap = new MinHeapImpl<>();
        for (int i = 1; i < 12; i++) {
            heap.insert(i); // Fill to capacity
        }
    }
}
