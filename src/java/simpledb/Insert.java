package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private final TupleDesc td;

    //helper for fetchNext
    private int counter;
    private boolean called;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // class Insert的构造函数：把从child operator中读取到的tuples添加到tableId对应的表中。
        // 有三个参数，第一个参数是代表transaction的tid，
        // 第二个参数是OpIterator类型的迭代器child，第三个参数是tableId。
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("tableId not matched");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"num of inserted tuples"});
        this.called = false;
        this.counter = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.counter = 0;
        this.called = false;
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.counter = 0;
        this.called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // fetchNext()：利用OpIterator类型的迭代器child找到一组要添加的记录
        // insert需要经过BufferPool，所以使用Database.getBufferPool().insertTuple(this.tid, this.tableId, t)方法进行添加。
        if(this.called) return null;
        this.called = true;
        while(this.child.hasNext()){
            Tuple t = this.child.next();
            try{
                Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
                this.counter ++;
            } catch (IOException e){
                e.printStackTrace();
                break;
            }
        }
        Tuple res = new Tuple(this.td);
        res.setField(0, new IntField(this.counter));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
