package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.Stack;

public class StackImpl<T> implements Stack<T> {
    // node class
    private static class Node<T> {
        Node<T> next;
        T element;
        public Node(T elem) {
            next = null;
            element = elem;
        }
    }

    // variables
    private int count;
    private Node<T> top;

    public StackImpl()
    {
        this.count = 0;
        this.top = null;
    }

    // push
    @Override
    public void push(T element) {
        Node<T> temp = new Node<>(element);
        temp.next = top;
        this.top = temp;
        count++;
    }

    // pop
    @Override
    public T pop() {
        if (this.size() == 0)
            return null;
        T result = this.peek();
        top = top.next;
        count--;
        return result;
    }

    // peek
    @Override
    public T peek() {
        if (this.size() == 0)
                return null;
        return this.top.element;
    }

    // stack size
    @Override
    public int size() {
        return count;
    }
}