package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    private TransactionId dirtyId;
    private boolean dirty;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    //HeapPage的构造函数：有两个参数，第一个是HeapPageId，第二个是byte[] 类型的data。
    // 在构造函数中为data分配空间，并将空间中开始部分用于header，header中保存了此页slots的bitmap信息。
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // getNumTuples()：返回此页中能够存储的tuple数量，用于构造函数中分配slot。
        //每页元组数 = floor((页大小*8)/(元组大小*8+1))
        return (int)Math.floor((BufferPool.getPageSize() * 8)/(td.getSize() * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // getHeaderSize()：返回此页需要多少bytes作为header。
        return (int)Math.ceil(this.numSlots * 1.0 / 8);

    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        // getBeforeImage()：在修改前返回此页的view，用于recovery。
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
    //throw new UnsupportedOperationException("implement this");
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // readNextTuple(DataInputStream dis, int slotId)：寻找到下一个被占用的slot，返回读取的tuple。
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        // getPageData()：返回byte[] 类型的此页数据。
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // deleteTuple(Tuple t)：从此页中删除特定的tuple数据，同时修改header中对应的bit，指示此slot处的数据已经被删除了。
        // necessary for lab2!
        int tupno = t.getRecordId().getTupleNumber();
        if(!isSlotUsed(tupno))
            throw new DbException("the tuple slot is empty!");
        if(tuples[tupno] == null)
            throw new DbException("the tuple is not in this page!");
        markSlotUsed(tupno, false);
        tuples[tupno] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // insertTuple(Tuple t)：利用getFirstNotUsedSlot()函数找到第一个空闲的slot，加入数据，同时修改header中对应的bit，指示此slot处有数据。
        //  necessary for lab2
        if(!t.getTupleDesc().equals(td))
            throw new DbException("tupledesc is dismatch!");
        if(getNumEmptySlots() == 0)
            throw new DbException("the page is emp");
        for(int i = 0; i < numSlots; i ++){
            if(!isSlotUsed(i)){
                t.setRecordId(new RecordId(pid, i));
                markSlotUsed(i, true);
                tuples[i] = t;
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // markDirty(boolean dirty, TransactionId tid)：标识此页的dirty/not dirty状态，同时利用tid说明是哪一个Transaction做了该标识。
	//  necessary for lab2
        this.dirty = dirty;
        this.dirtyId = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // TransactionId isDirty()：返回最后一个dirtied了此页的Transaction的tid，如果不dirty返回null。
	//  necessary for lab2
        return this.dirty ? this.dirtyId : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // getNumEmptySlots()：返回此页中为空的slot数量
        int cnt = 0;
        for(int i = 0; i < numSlots; i ++){
            if(!isSlotUsed(i)){cnt ++;}
        }
        //System.out.println(cnt);
        return cnt;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // isSlotUsed(int i)：判断下标为i的slot是否被占用。
        //bitmap中，low bits代表了先填入的slots状态。
        // 因此，第一个headerByte的最小bit代表了第一个slot是否使用，
        // 第二小的bit代表了第二个slot是否使用。
        // 同样，最大headerByte的一些高位可能不与slot存在映射关系，
        // use bitmap
        int q = i >> 3;
        int r = i & 7;
        int bitind = header[q];
        return (((bitind >> r) & 1) == 1);
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // markSlotUsed(int i, boolean value)：在header中标识下标为i的slot是否被占用，true在header的bit位上标1，false则标0。
        //necessary for lab2
        byte b = header[Math.floorDiv(i, 8)];
        byte mask = (byte)(1 << (i & 7));
        if(value)
            header[Math.floorDiv(i, 8)] = (byte)(b | mask);
        else
            header[Math.floorDiv(i, 8)] = (byte)(b & (~mask));
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        ArrayList<Tuple> filledTuples = new ArrayList<Tuple>();
        for(int i = 0; i < numSlots; i ++){
            if(isSlotUsed(i)){
                filledTuples.add(tuples[i]);
            }
        }
        return filledTuples.iterator();
    }

}

