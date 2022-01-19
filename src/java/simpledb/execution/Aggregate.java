package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Table;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    Aggregator aggregator;
    OpIterator child;
    private int gfield;
    private int afield;
    private Aggregator.Op op;
    private TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        Type fieldType = child.getTupleDesc().getFieldType(afield);
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = aop;
        if (fieldType == Type.INT_TYPE) aggregator = new IntegerAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
        else aggregator = new StringAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here

        if (gfield != Aggregator.NO_GROUPING) return gfield;

        return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    OpIterator opIterator;
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        opIterator = aggregator.iterator();
        opIterator.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        opIterator.rewind();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (opIterator.hasNext()) return opIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (td != null) return td;
        Type[] types;
        String[] alias;
        if (gfield == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            alias = new String[]{op.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
        }else {
            types = new Type[]{child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE};
            alias = new String[]{child.getTupleDesc().getFieldName(gfield),
                    op.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
        }
        td = new TupleDesc(types, alias);
        return td;
    }

    public void close() {
        // some code goes here
        opIterator.close();
        super.close();
        opIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children == null || children.length <= 0) return;
        child = children[0];
    }
}
