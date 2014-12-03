package org.gestore;

import org.gestore.move;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class moveTest 
    extends TestCase
{
    move mover;
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public moveTest( String testName )
    {
        super( testName );
        mover = new move();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}
