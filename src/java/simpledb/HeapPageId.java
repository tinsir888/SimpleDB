package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    // add new variable
    private final int tableId;
    private final int pgNo;

    public HeapPageId(int tableId, int pgNo) {
        //HeapPageId的构造函数：对于特定table中的一页特定Page，设置一个page id 结构，参数为int类型的tableId和int类型的pgNo。
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // getTableId()：返回PageId对应的tableId。
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // getPageNumber()：返回tableId对应的表包含的page数量。
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        String hash = "" + tableId + pgNo;
        return hash.hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // equals(Object o)：判断两个pageId对象是否相等。
        if(this.getClass().isInstance(o)){
            PageId pgid = (PageId) o;
            if(pgid.getTableId() == tableId && pgid.getPageNumber() == pgNo){
                return true;
            }
            else return false;
        }
        else return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
