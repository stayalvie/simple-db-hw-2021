package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return data.iterator();
    }

    private static final long serialVersionUID = 1L;

    /*
    * TODO 可能后面需要进行重构 ， 如果不能利用容器
    * */
    private CopyOnWriteArrayList<TDItem> data;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr == null || fieldAr == null) {
            System.out.println("数组越界");
            return ;
        }
        this.data = new CopyOnWriteArrayList<>();
        for (int i = 0; i < typeAr.length; i ++) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            data.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return data.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return data.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return data.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int i = 0;
        Iterator<TDItem> itemIterator = iterator();
        while (itemIterator.hasNext()) {
            TDItem tdItem = itemIterator.next();

            if (tdItem.fieldName == null) {
                if (name == null) return i;
                else continue;
            }else {
                if (tdItem.fieldName.equals(name)) return i;
            }
            i ++;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int res = 0;
        Iterator<TDItem> itemIterator = iterator();
        while (itemIterator.hasNext()) {
            TDItem tdItem = itemIterator.next();
            res += tdItem.fieldType.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        Iterator<TDItem> td1Iter = td1.iterator();
        Iterator<TDItem> td2Iter = td2.iterator();

        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];
        int i = 0;
        while (td1Iter.hasNext()) {
            TDItem tdItem = td1Iter.next();
            types[i] = tdItem.fieldType;
            names[i ++] = tdItem.fieldName;
        }
        while (td2Iter.hasNext()) {
            TDItem tdItem = td2Iter.next();
            types[i] = tdItem.fieldType;
            names[i ++] = tdItem.fieldName;
        }

        return new TupleDesc(types, names);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null) return false;
        if (o instanceof TupleDesc) {

            TupleDesc t = (TupleDesc) o;
            if (t.getSize() != this.getSize()) return false;
            Iterator<TDItem> tIterator = t.iterator();
            Iterator<TDItem> iterator = this.iterator();
            while (iterator.hasNext()) {
                TDItem next = tIterator.next();
                TDItem next1 = iterator.next();
                if (next.fieldType != next1.fieldType) return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder res = new StringBuilder();
        Iterator<TDItem> iterator = this.iterator();
        while (iterator.hasNext()) {
            TDItem t = iterator.next();
            res.append(t.fieldType.toString() + "(");
            if (t.fieldName == null) res.append("null");
            else res.append(t.fieldName);
            res.append(")" + ",");
        }
        if (res.length() == 0) return res.toString();

        return res.substring(0, res.length() - 1);
    }
}
