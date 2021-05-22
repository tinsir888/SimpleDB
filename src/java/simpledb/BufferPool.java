package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓冲池（SimpleDB中的类“ BufferPool”）负责
 * 用于将最近从磁盘读取的页面缓存在内存中。 全部
 * 操作员通过缓冲区从磁盘上的各种文件读取和写入页面
 * 水池。 它由固定数量的页面组成，由
 * BufferPool构造函数的numPages参数。
 * 在以后的实验中，您将实现一个
 * 驱逐政策。 在本实验中，您只需实现构造函数，然后
 * SeqScan运算符使用的“ BufferPool.getPage（）”方法。
 * BufferPool最多可存储`numPages`页。 为了这
 * 实验室，如果针对不同的请求提出了超过“ numPages”个请求
 * 页，然后可以实施一个驱逐策略，而不是实施驱逐策略
 * DbException。 在以后的实验室中，您将需要搬迁
 * 政策。
 * “数据库”类提供了一个静态方法，
 * Database.getBufferPool（），返回对单个对象的引用
 * 整个SimpleDB流程的BufferPool实例。
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

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pageStore;
    private final ConcurrentHashMap<PageId, Integer> pageAge;//add for lab 4
    private int age;//add for lab 4
    private PageLockManager lockManager;//add for lab 4

    private class Lock{//add for lab 4
        TransactionId tid;
        int lockType;//0: shared lock, 1: exclusive lock

        public Lock(TransactionId tid, int lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
        @Override
        public boolean equals(Object o){
            if(this == o) return true;
            if(o == null || getClass() != o.getClass()) return false;
            Lock o_lock = (Lock) o;
            return tid.equals(o_lock.tid) && lockType == o_lock.lockType;
        }
    }
    private class PageLockManager{// add for lab4
        ConcurrentHashMap<PageId,Vector<Lock>> lockMap;
        public PageLockManager(){
            lockMap = new ConcurrentHashMap<PageId,Vector<Lock>>();
        }
        public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType){
            if(lockMap.get(pid) == null){//if no lock held on pid
                Lock lock = new Lock(tid,lockType);
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                lockMap.put(pid,locks);
                return true;
            }
            // if some Tx holds lock on pid
            // locks.size() won't be 0 because releaseLock will remove 0 size locks from lockMap
            Vector<Lock> locks = lockMap.get(pid);
            // if tid already holds lock on pid
            for(Lock lock:locks){
                if(lock.tid == tid){
                    // already hold that lock
                    if(lock.lockType == lockType)
                        return true;
                    // already hold exclusive lock when acquire shared lock
                    if(lock.lockType == 1)
                        return true;
                    // already hold shared lock,upgrade to exclusive lock
                    if(locks.size()==1){
                        lock.lockType = 1;
                        return true;
                    }else{
                        return false;
                    }
                }
            }
            // if the lock is a exclusive lock
            if (locks.get(0).lockType ==1){
                assert locks.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }
            // if no exclusive lock is held, there could be multiple shared locks
            if(lockType == 0){
                Lock lock = new Lock(tid,0);
                locks.add(lock);
                lockMap.put(pid,locks);
                return true;
            }
            // can not acquire a exclusive lock when there are shard locks on pid
            return false;
        }
        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            // if not a single lock is held on pid
            assert lockMap.get(pid) != null : "page not locked!";
            Vector<Lock> locks = lockMap.get(pid);
            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);
                // release lock
                if(lock.tid == tid){
                    locks.remove(lock);
                    // if the last lock is released
                    // remove 0 size locks from lockMap
                    if(locks.size() == 0)
                        lockMap.remove(pid);
                    return true;
                }
            }
            // not found tid in tids which lock on pid
            return false;
        }

        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            // if not a single lock is held on pid
            if(lockMap.get(pid) == null)
                return false;
            Vector<Lock> locks = lockMap.get(pid);
            // check if a tid exist in pid's vector of locks
            for(Lock lock:locks){
                if(lock.tid == tid){
                    return true;
                }
            }
            return false;
        }
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // BufferPool(int numPages)：BufferPool的构造函数，创建一个BufferPool实例缓存最大numPages数量的Pages，通过<PageId,Page>类型的pageStore哈希表管理缓存pages。
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<PageId, Page>();
        pageAge = new ConcurrentHashMap<PageId, Integer>();
        age = 0;
        lockManager = new PageLockManager();
    }
    // getPageSize()：获得每个Page大小，默认是4096。
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // getPage(TransactionId tid, PageId pid, Permissions perm)：根据pid获取Page，如果在pageStore中，返回对应Page;
        // 如果不在就添加进哈希表，如果缓存的page数量超过缓存最大numPages数量，调用evictPage()淘汰一个页。
        // 获得page时在tid代表的Transaction上加锁，perm代表锁的类型，保证使用返回Page时的安全性。
        int lockType;
        if (perm == Permissions.READ_ONLY){
            lockType = 0;
        } else lockType = 1;
        boolean lockAcquired = false;
        //---------------------------------------------------
        if(!pageStore.containsKey(pid)){
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbfile.readPage(pid);
            if(pageStore.size() == numPages){
                evictPage();
            }
            pageStore.put(pid, page);
            pageAge.put(pid, age ++);
            return page;
        }
        return pageStore.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /*
     * add by tinsir888
     * restorePages
     */
     private synchronized void restorePages(TransactionId tid){
         for (PageId pid : pageStore.keySet()){
             Page page = pageStore.get(pid);
             if(page.isDirty() == tid){
                 int tableId = pid.getTableId();
                 DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                 Page pageFromDisk = file.readPage(pid);
                 pageStore.put(pid, pageFromDisk);
             }
         }
     }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            flushPages(tid);
        } else{
            restorePages(tid);
        }
        for(PageId pid : pageStore.keySet()){
            if(holdsLock(tid, pid))
                releasePage(tid, pid);
        }
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
        // insertTuple(TransactionId tid, int tableId, Tuple t)：
        // 在BufferPool中添加特定的tuple到tableId对应的表中，
        // 调用DbFile的insertTuple(tid, t)方法（其中有一个读写锁），并将添加了tuple的page mark dirty。
        // necessary for lab2
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(f.insertTuple(tid, t), tid);
    }

    private void updateBufferPool(ArrayList<Page> pagelist, TransactionId tid) throws DbException {
        for(Page p: pagelist){
            p.markDirty(true, tid);
            if(pageStore.size() > numPages){
                evictPage();
            }
            pageStore.put(p.getId(), p);
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
        // deleteTuple(TransactionId tid, Tuple t)：从BufferPool中删除特定的tuple，
        // 调用DbFile的deleteTuple(tid, t)方法（其中有一个读写锁），并将删除了tuple的page mark dirty。
        // necessary for lab2
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(f.deleteTuple(tid, t), tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // necessary for lab2
        for(Page p: pageStore.values()){
            flushPage(p.getId());
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
        // discardPage(PageId pid)：从BufferPool的缓存中删除pid对应的page。
        // not necessary for lab1
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // flushPage(PageId pid)：将pid对应的Page从BuffePool的缓存中写入disk。
        // not necessary for lab1
        Page p = pageStore.get(pid);
        TransactionId tid = null;
        if((tid = p.isDirty()) != null){
            Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2, for lab4
        for(PageId pid : pageStore.keySet()){
            Page page = pageStore.get(pid);
            if(page.isDirty() == tid){
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // evictPage()：当缓存的page数量超过缓存最大numPages数量，调用evictPage()淘汰一个页。
        // 先维护一个<PageId,Integer>类型的哈希表pageAge，根据Page载入cache的时间排序，淘汰缓存中最老的Page
        // necessary for lab2
        /*PageId pid = new ArrayList<>(pageStore.keySet()).get(0);
        try{
            flushPage(pid);
        } catch (IOException e){
            e.printStackTrace();
        }
        discardPage(pid);*/
        assert numPages == pageStore.size() : "Bufferpool is not full, no need to evict.";
        PageId pgid = null;
        int oldestAge = -1;
        // find the oldest page to evict (which is not dirty)
        for(PageId pid : pageAge.keySet()){
            Page page = pageStore.get(pid);
            // skip dirty page
            if(page.isDirty() != null){
                continue;
            }
            if(pgid == null){
                pgid = pid;
                oldestAge = pageAge.get(pid);
                continue;
            }
            if(pageAge.get(pid) < oldestAge){
                pgid = pid;
                oldestAge = pageAge.get(pid);
            }
        }
        if(pgid == null)
            throw new DbException("all pages are not dirty");
        Page page = pageStore.get(pgid);

        //evict
        pageStore.remove(pgid);
        pageAge.remove(pgid);
    }
}
