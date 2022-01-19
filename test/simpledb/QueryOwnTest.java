package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.sql.Types;

/**
 * @author xiaofei
 * @create 2022-01-16 21:13
 */
public class QueryOwnTest {


    public static void main(String[] args) {
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};

        String[] alias = new String[]{"f1", "f2", "f3"};

        TupleDesc td = new TupleDesc(types, alias);

        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);

        Database.getCatalog().addTable(table1, "t1");
        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        Filter sf1 = new Filter(new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);

        Join j = new Join(p, sf1, ss2);

        try {
            j.open();
            while (j.hasNext()) {
                Tuple t = j.next();
                System.out.println(t);
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            j.close();
            Database.getBufferPool().transactionComplete(tid);
        }


    }
}