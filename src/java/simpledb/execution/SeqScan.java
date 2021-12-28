package simpledb.execution;

import simpledb.common.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here

        this.tableAlias = tableAlias;
        this.tableId = tableid;
        this.tid = tid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableAlias = tableAlias;
        this.tableId = tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    private int curPageNo;
    private Iterator<Tuple> curPage;
    private int totalPagesNum;
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        curPageNo = 0;
        HeapPage page = (HeapPage) Database.getBufferPool()
                .getPage(tid, new HeapPageId(tableId, curPageNo), Permissions.READ_ONLY);
        HeapFile databaseFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        totalPagesNum = databaseFile.numPages();
        curPage = page.iterator();
        curPageNo ++;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc desc = Database.getCatalog().getTupleDesc(tableId);
        int fieldNums = desc.numFields();
        Type[] fieldType = new Type[fieldNums];
        String[] aliasName = new String[fieldNums];
        Iterator<TupleDesc.TDItem> iterator = desc.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            TupleDesc.TDItem next = iterator.next();
            fieldType[i] = next.fieldType;
            String sb = Objects.requireNonNullElse(tableAlias, "null") +
                    "." +
                    Objects.requireNonNullElse(next.fieldName, "null");
            aliasName[i ++] = sb;
        }
        return new TupleDesc(fieldType, aliasName);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (curPage == null) throw new IllegalStateException("close");
        if (!curPage.hasNext() && curPageNo < totalPagesNum ) {
            HeapPage page = (HeapPage) Database.getBufferPool()
                    .getPage(tid, new HeapPageId(tableId, curPageNo), Permissions.READ_ONLY);
            curPage = page.iterator();
            curPageNo ++;

        }
        return curPage.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!hasNext()) throw new NoSuchElementException("no next Tuple");
        return curPage.next();
    }

    public void close() {
        // some code goes here
        curPage = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (curPage == null) throw new IllegalStateException("close");
        open();
    }
}
