package edu.yu.cs.com1320.project;

import edu.yu.cs.com1320.project.impl.StackImpl;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class StackImplTest {
    // constructor and empty stack test
    @Test
    public void testConstructorAndEmptyStack() {
        Stack<Integer> intStack = new StackImpl<>();
        assertEquals(0,intStack.size());
        assertNull(intStack.pop());
    }
    // push test
    @Test
    public void testPush() {
        Stack<Integer> intStack = new StackImpl<>();
        intStack.push(14);
        assertEquals(14, intStack.peek());
        assertEquals(1, intStack.size());
    }
    // pop test
    @Test
    public void testPop() {
        Stack<String> strStack = new StackImpl<>();
        strStack.push("bro");
        strStack.push("isn't");
        strStack.push("this");
        assertEquals("this", strStack.pop());
        strStack.push("so");
        strStack.push("cool");
        assertEquals(4, strStack.size());
        assertEquals("cool", strStack.pop());
        strStack.pop();
        strStack.pop();
        assertEquals("bro", strStack.pop());
        assertEquals(0, strStack.size());
        assertNull(strStack.pop());
    }
}
