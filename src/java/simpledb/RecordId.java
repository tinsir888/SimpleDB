package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private final PageId pid;
    private final int tupleno;
    public RecordId(PageId pid, int tupleno) {
        // RecordId的构造函数：RecordId是对一个特定Table中特定一个Page的一个特定tuple的引用，
        // 构造时用到的参数是PageId类型的 pid 和 int类型的 tupleno，其中pid是该tuple所在的Page对应的Id，tupleno是该tuple是该Page中第几个。

        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // getTupleNumber()：返回RecordId引用tuple的tupleno。
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // getPageId()：返回RecordId引用tuple的pid
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // equals(Object o)：判断两个RecordId对象是否相等。
        if(this.getClass().isInstance(o)) {
            RecordId rc = (RecordId) o;
            if (rc.getPageId().equals(pid) && rc.getTupleNumber() == tupleno) {
                return true;
            } else return false;
        }
        else return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        String hash = "" + pid.getTableId() + pid.getPageNumber() + tupleno;
        return hash.hashCode();
    }

}
