package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import javax.xml.stream.events.DTD;
import java.lang.management.MemoryUsage;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;    //group by 后面字段 的索引
    private Type gbFieldType;   //类型
    private int afield; //聚合字段 索引
    private Op what; // 执行什么样子的聚合操作
    private Map<Field, Integer> aResult; // 有GB分组的结果以及对应的Tuple
    private Map<Field, Integer> count; //得到对应的数量用于avg
    private String gbName;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        aResult = new HashMap<>();
        count = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        gbName = gbField == NO_GROUPING ? tup.getTupleDesc().getFieldName(gbField) : "";
        Field index = gbField == NO_GROUPING ? new IntField(-1) : tup.getField(gbField);
        IntField field = (IntField) tup.getField(afield);
        Integer value = 0;
        switch (what) {
            case SUM:;
            case AVG:
                value = aResult.getOrDefault(index, 0);
                value += field.getValue();
                int c = count.getOrDefault(index, 0);
                c ++;
                count.put(index, c);
                break;
            case MAX:
                value = field.getValue();
                if (aResult.containsKey(index)) {
                    value = Math.max(value, aResult.get(index));
                }
                break;
            case MIN:
                value = field.getValue();
                if (aResult.containsKey(index)) {
                    value = Math.min(value, aResult.get(index));
                }
                break;
            case COUNT:
                value = 1;
                if (aResult.containsKey(index)) {
                    value += aResult.get(index);
                }
                break;
            case SC_AVG:
            case SUM_COUNT:
            default:
                System.out.println("no Op");
        }
        aResult.put(index, value);
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator(){

            Iterator<Map.Entry<Field, Integer>> entry;
            TupleDesc td;
            boolean flag = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                entry = aResult.entrySet().iterator();
                Type[] types;
                String[] strings;
                if (gbField == NO_GROUPING){
                    types = new Type[]{Type.INT_TYPE};
                    strings = new String[]{what.toString()};
                }else {
                    types = new Type[]{gbFieldType, Type.INT_TYPE};
                    strings = new String[]{gbName, what.toString()};
                }
                if (td == null)
                    td = new TupleDesc(types, strings);
                flag = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!flag) throw new DbException("no open");
                return entry.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!flag) throw new DbException("no open");
                if (!hasNext()) throw new NoSuchElementException();
                Tuple tuple;
                Map.Entry<Field, Integer> next = entry.next();
                tuple = new Tuple(td);
                int index;
                if (td.numFields() == 1) index = 0;
                else {
                    tuple.setField(0, next.getKey());
                    index = 1;
                }
                if (what == Op.AVG){
                    tuple.setField(index, new IntField(next.getValue() / count.get(next.getKey())));
                }else
                    tuple.setField(index, new IntField(next.getValue()));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!flag) throw new DbException("no open");
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                entry = null;
            }
        };
    }
}
