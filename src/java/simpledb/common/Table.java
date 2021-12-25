package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.TupleDesc;

/**
 * @author xiaofei
 * @create 2021-12-24 21:51
 */
public class Table {

    public Table(DbFile file, String name, String pkeyField){
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
        this.tupleDesc = file.getTupleDesc();
    }
    public DbFile file;
    public String name;
    public String pkeyField;
    public TupleDesc tupleDesc;
}
