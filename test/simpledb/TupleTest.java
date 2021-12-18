package simpledb;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;

import java.util.Iterator;

public class TupleTest extends SimpleDbTestBase {

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test public void modifyFields() {
        TupleDesc td = Utility.getTupleDesc(2);
        td.toString();
        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));

        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));
    }

    /**
     * Unit test for Tuple.getTupleDesc()
     */
    @Test public void getTupleDesc() {
        TupleDesc td = Utility.getTupleDesc(5);
        Tuple tup = new Tuple(td);
        System.out.println(tup.toString());
        assertEquals(td, tup.getTupleDesc());

    }

    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0,0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

        try {
            assertEquals(rid1, tup1.getRecordId());
        } catch (java.lang.UnsupportedOperationException e) {
            //rethrow the exception with an explanation
            throw new UnsupportedOperationException("modifyRecordId() test failed due to " +
                    "RecordId.equals() not being implemented.  This is not required for Lab 1, " +
                    "but should pass when you do implement the RecordId class.");
        }
    }

    /**
     *
     */
    @Test public void testIterator(){

        Tuple tup1 = new Tuple(Utility.getTupleDesc(3));

        Iterator<Field> fields = tup1.fields();

        while (fields.hasNext()) {
            Field field = fields.next();
            System.out.println(field);
        }

    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleTest.class);
    }
}

