package simpledb.storage;

import org.w3c.dom.Node;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.security.Permission;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private int numPages;
    /*
    *   TODO: 是否进行重构存储的容器， 到写 驱逐方法的时候进行重构
    * */
    private ConcurrentHashMap<PageId, PageNode> data;
    private ConcurrentHashMap<PageId, ControllerPageMsg> controllerMsg;




    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        data = new ConcurrentHashMap<>();
        controllerMsg = new ConcurrentHashMap<>();
        head = new PageNode();
        tail = new PageNode();
        head.next = tail;
        tail.pre = head;
        lock =  new Object();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    /*
    *
    * TODO: 这里面的设计暂且还待定
    * */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        /*
        * TODO 如何书写驱逐方法? 读写锁吧， 太多东西要考虑了？
        *
        *
        * 以前的方案：
        *    1. concurrentHashmap  + concurrentLinkedQueue, 无法实现因为size不准确
        *    2. 自己实现双链表， 有无办法更细粒度的锁 ？？ 目前没有想到
        *
        * 初步方案：
        *    1. 大锁
        *    2. 在淘汰页面的时候应该考虑 当前页面是否有事务
        *
        *
        * */

        synchronized (lock) {
            if (!data.containsKey(pid)) {
                    if (data.size() >= numPages) {
                        evictPage();
                    }
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page page = dbFile.readPage(pid);
                    PageNode pageNode = new PageNode(pid, page);
                    data.put(pid, pageNode);
                    if (!controllerMsg.containsKey(pid))
                        controllerMsg.put(pid, new ControllerPageMsg());
                    insertNode(pageNode);
            }
            /*
             * 对页面的锁如何去写？？？？
             *  完成TDO
             * */
            ControllerPageMsg controllerPageMsg = controllerMsg.get(pid);
            if (perm == Permissions.READ_ONLY) {

                try {
                    controllerPageMsg.rw.readLock().lockInterruptibly();
                    controllerPageMsg.ownReader.add(tid);
                    /*
                     * TODO: 解锁
                     * */
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new TransactionAbortedException();
                }
            } else {
                try {
                    controllerPageMsg.rw.writeLock().lockInterruptibly();
                    controllerPageMsg.writerPidLocker = tid;
                    /*
                     * TODO: 解锁
                     * */
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new TransactionAbortedException();
                }
            }

            return data.get(pid).page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *   ??? deadLock? 需要强制释放获得到的资源？
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2


    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2

    }

    public void releaseLock(TransactionId tid, PageId pid, Permissions p) {

        ControllerPageMsg controllerPageMsg = controllerMsg.get(pid);

        if (controllerPageMsg == null) return;
        if (p == Permissions.READ_ONLY) {
            controllerPageMsg.ownReader.remove(tid);
            controllerPageMsg.rw.readLock().unlock();
        }else {
            controllerPageMsg.writerPidLocker = null;
            controllerPageMsg.rw.writeLock().unlock();
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = databaseFile.insertTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            if (!data.containsKey(p.getId())) {
                data.put(p.getId(), new PageNode(p.getId(), p));
                controllerMsg.put(p.getId(), new ControllerPageMsg());
            }
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);

        databaseFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageNode pageNode : data.values()) {
            Database.getCatalog().getDatabaseFile(pageNode.pageId.getTableId()).writePage(pageNode.page);
        }
    }



    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (!data.containsKey(pid)) return;
        PageNode pageNode = data.get(pid);
        removeNode(pageNode);
        data.remove(pageNode.pageId);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = data.get(pid).page;
        if (page.isDirty() != null) {
            DbFile databaseFile =
                    Database.getCatalog().getDatabaseFile(pid.getTableId());
            databaseFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException{
        // some code goes here
        // not necessary for lab1
        PageNode t = tail.pre;
        while (t != head) {
            Page page = data.get(t.pageId).page;
            if (page.isDirty() == null) {
                break;
            }
            t = t.pre;
        }
        if (t == head) throw new DbException("dirty to many");
        discardPage(t.pageId);
    }


    /*
    * TODO: do more controller Msg . for example: dityPage
    *
    * 原来人家已经写好类了， 是我写之前想太多了， 但是对于他的那个还是感觉没有dityPage
    *
    * 还是上面的 TODO思考
    *
    * 迭代
    *   完成TODO 抄袭mysql的理论 每一个页面都有起对应的控制信息
    * */
    static class ControllerPageMsg{
        ReadWriteLock rw;
        TransactionId writerPidLocker;
        Set<TransactionId> ownReader;

        public ControllerPageMsg() {
            this.rw = new ReentrantReadWriteLock();
            this.ownReader = new HashSet<>();
        }
    }



    /*
     * 一下为实现驱逐策略
     *
     * TODO: 有没有更细粒度的锁来进行实现?
     *  目前是没有办法的因为我们在去判断size的时候 还有一个问题就是 我淘汰的时候size的原子性子么保证
     *
     * */
    // 将某一个页面从中间删除
    private void moveToHead(PageNode node) {
        removeNode(node);
        insertNode(node);
    }
    //一定插入头部
    private void insertNode(PageNode node) {
//        head.lock.lock();
//        try {
//            node.next = head.next;
//            node.pre = head;
//            head.next.lock.lock();  //改变了 head.next。pre所以要对head.next进行获取锁
//            try {
//                head.next.pre = node;
//            } finally {
//                head.next.lock.unlock();
//            }
//            head.next = node;
//        } finally {
//            head.lock.unlock();
//        }
        node.next = head.next;
        node.pre = head;
        head.next.pre = node;
        head.next = node;
    }

    //删除尾部
    private PageNode removeTail() {
        return removeNode(tail.pre);
    }

    private PageNode removeNode(PageNode node) {
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.pre = null;
        node.next = null;
        return node;
    }

    private PageNode head;
    private PageNode tail;
    private final Object lock; //改变双向链表中间节点的时候需要获取的节点
    class PageNode {
        PageId pageId;
        Page page;
        PageNode next;
        PageNode pre;

        public PageNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
        public PageNode() {
        }

    }
}
