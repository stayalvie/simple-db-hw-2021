package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int afield;
    private Op operation;
    private Map<Field, Integer> aResult;
    private String gbname;
    private int count; // 标识没有分组字段的count则为全部数据
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException("what != count");
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.operation = what;
        aResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tup.getField(afield) == null) return;
        if (Op.COUNT == operation) {
            if (gbField == NO_GROUPING) {
                count ++;
                return ;
            }
            gbname = tup.getTupleDesc().getFieldName(gbField);
            Field field = tup.getField(gbField);
            int sum = 1;
            if (aResult.containsKey(field)) {
                sum += aResult.get(field);
            }
            aResult.put(field, sum);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here


        return new OpIterator() {
            TupleDesc td;
            Iterator<Map.Entry<Field, Integer>> iterator;
            boolean flag;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                Type[] types;
                String[] name;
                if (gbField == NO_GROUPING) {
                    types = new Type[]{Type.INT_TYPE};
                    name = new String[]{operation.toString()};
                }else {
                    types = new Type[]{gbFieldType, Type.INT_TYPE};
                    name = new String[]{gbname, operation.toString()};
                }
                td = new TupleDesc(types, name);
                iterator = aResult.entrySet().iterator();
                flag = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!flag) throw new DbException("iterator has closed");
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new DbException("no Tuple");
                Tuple t = new Tuple(td);
                Map.Entry<Field, Integer> next = iterator.next();
                if (gbField == NO_GROUPING) {
                    t.setField(0, new IntField(next.getValue()));
                }else {
                    t.setField(0, next.getKey());
                    t.setField(1, new IntField(next.getValue()));
                }
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!flag) throw new DbException("no Tuple");
                iterator = aResult.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                flag = false;
                iterator = null;
                td = null;
            }
        };
    }

}
