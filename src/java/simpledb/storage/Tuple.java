package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private Field[] fields;
    private TupleDesc tupleDesc;
    private RecordId recordId;
    public Tuple(TupleDesc td) {
        // some code goes here
        fields = new Field[td.numFields()];
        tupleDesc = td;
    }

//    private Field getField(Type type) {
//        switch (type) {
//            case INT_TYPE:return new IntField();
//            case STRING_TYPE:
//        }
//    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= fields.length) {
            System.out.println("数组越界");
            return ;
        }
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i >= fields.length) return null;
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here

        StringBuilder sb = new StringBuilder();
        for (Field field : fields) {
            if (field == null) sb.append("null");
            else sb.append(field.toString());
            sb.append("\t");
        }
        if (sb.length() == 0) return "";

        return sb.substring(0, sb.length() - 1);
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     *
     *        TODO: 数组迭代器
     * */
    public Iterator<Field> fields() {
        // some code goes here
        return new Itr();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        td = null;
    }



    class Itr implements Iterator<Field>{


        private int cursor;

        @Override
        public boolean hasNext() {
            return cursor != fields.length;
        }

        @Override
        public Field next() {

            int i = cursor ++;
            if (i >= fields.length) return null;
            return fields[i];
        }
    }


}
