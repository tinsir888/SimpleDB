package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     *            HeapFile的构造函数：通过特定的file文件构建一个heap file，
     *            有两个参数，第一个是File类型的f，第二个是TupleDesc类型的td。
     */
    private final File file;
    private final TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // getFile()：返回磁盘中支持此HeapFile 的File类型文件。
        return file;
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
        // getId()：返回唯一标识此HeapFile的ID。
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // getTupleDesc()：返回存储在这个DbFile中的table 的TupleDesc。
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // readPage(PageId pid)：读取pid对应的Page。先找到File内要读取的Page Number，读取整个Page返回。
        Page res = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try(RandomAccessFile raf = new RandomAccessFile(getFile(), "r")){
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(data, 0, data.length);
            res = new HeapPage((HeapPageId)pid, data);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return  res;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // writePage(Page page)：写pid对应的Page。先找到File内要写的Page Number，写入整个Page。
        //  necessary for lab2
        int pgno = page.getId().getPageNumber();
        if(pgno > numPages())
            throw new IllegalArgumentException("invalid pgno");
        int pgsize = BufferPool.getPageSize();
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(pgno * pgsize);
        byte[] data = page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // numPages()：返回这个HeapFile中包含的page数量。
        return (int)Math.floor(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // insertTuple(TransactionId tid, Tuple t)：找到一个未满的page，如果不存在空闲的slot，创建新的一页存储tuple，之后添加，返回添加过的Page。
        ArrayList<Page> res = new ArrayList<>();
        for(int i = 0; i < numPages(); i ++){
            HeapPage cur = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(cur.getNumEmptySlots() == 0)continue;
            cur.insertTuple(t);
            res.add(cur);
            return res;
        }
        res.clear();
        BufferedOutputStream tmp = new BufferedOutputStream(new FileOutputStream(this.file, true));
        tmp.write(HeapPage.createEmptyPageData());
        tmp.close();
        HeapPage cur = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        cur.insertTuple(t);
        res.add(cur);
        return res;
        // necessary for lab2
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // deleteTuple(TransactionId tid, Tuple t)：找到对应的page，删除tuple，标识此page为dirty。
        ArrayList<Page> res = new ArrayList<>();
        HeapPage cur = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        cur.deleteTuple(t);
        res.add(cur);
        return res;
        // necessary for lab2
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
    //add---
    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int thePage;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            thePage = 0;
            it = getPageTuples(thePage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d doesn't contains page %d.", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if(it == null){
                return false;
            }

            if(!it.hasNext()){
                if(thePage < (heapFile.numPages()-1)){
                    thePage++;
                    it = getPageTuples(thePage);
                    return it.hasNext();
                }else{
                    return false;
                }
            }else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            close();
            open();
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            it = null;
        }

    }
}

