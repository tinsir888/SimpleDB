package simpledb;

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
        // some code goes here
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
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
        int tid = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f = null;
        try{
            f = new RandomAccessFile(file, "r");
            if((pgNo + 1) * BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid.", tid, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());

            int read = f.read(bytes, 0, BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tid, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, bytes);
        }catch(IOException e){
            e.printStackTrace();
        }finally {
            try {
                f.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tid, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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

