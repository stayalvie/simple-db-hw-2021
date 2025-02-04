package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.dsig.keyinfo.PGPData;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f.getAbsoluteFile();
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            int pagesize = BufferPool.getPageSize();
            byte[] b = new byte[pagesize];
            raf.seek(pid.getPageNumber() * pagesize);
            raf.read(b, 0, pagesize);
            raf.close();
            return new HeapPage((HeapPageId)pid, b);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // some code goes here
        // not necessary for lab1
        //System.out.println("write page :"+page.getId().getPageNumber());
        int pgno=page.getId().getPageNumber();
        byte[] data=page.getPageData();

        RandomAccessFile rf=new RandomAccessFile(f, "rw");
        if(pgno>numPages()) {
            rf.close();
            throw new IllegalArgumentException();
        }
        rf.seek(pgno*BufferPool.getPageSize());
        rf.write(data, 0, BufferPool.getPageSize());
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        List<Page> pages = new ArrayList<>();

        for (int i = 0; i < numPages(); i ++) {
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
            if (p.getNumEmptySlots() == 0) {
                //释放 当前页的读的权限
                Database.getBufferPool().releaseLock(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
            }else {
                //
                Database.getBufferPool().releaseLock(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
                page.insertTuple(t);
                page.markDirty(true, tid);
                pages.add(page);
                return pages;
            }
        }

        // 本文件没有对应的页或者所有页都已经满了需要开辟新的页
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()), Permissions.READ_WRITE);

        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();

        p.insertTuple(t);
        //writePage(p); 目前功能没有实现

        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        Database.getBufferPool().releaseLock(tid,t.getRecordId().getPageId(), Permissions.READ_WRITE);
        return pages;
        // not necessary for lab1
    }

    /*
    * TODO: Iterator如何去优化
    * */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private TransactionId tid;
            private int pageNo;
            private Iterator<Tuple> tupleItr;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY);
                tupleItr = page.iterator();
                ++ pageNo;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleItr == null) throw new IllegalStateException("close");
                if (!tupleItr.hasNext() && pageNo >= numPages()) return false;

                while (!tupleItr.hasNext() && pageNo < numPages()) {
                    HeapPage p = (HeapPage) Database.getBufferPool()
                            .getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY);
                    tupleItr = p.iterator();
                    pageNo ++;
                }
                return tupleItr.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException("no tuple");
                return tupleItr.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (tupleItr == null) throw new IllegalStateException("close");
                open();
            }

            @Override
            public void close() {
                tupleItr = null;
            }
        };
    }

}

