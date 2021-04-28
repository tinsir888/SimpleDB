package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;

    private Aggregator agg;
    private OpIterator it;
    private Type gbfieldType;
    // Aggregate的构造函数：Aggregation operator用于计算一个Aggregate（e.g. sum,avg,max,min），
    // 我们需要对一列数据支持聚合。构造函数有四个参数，第一个参数是OpIterator类型的 child，用于不断提供tuples；
    // 第二个参数是 int 类型的 afield，标识着我们需要聚合的列；
    // 第三个参数是 int 类型的gfield，标识着结果中我们需要group by 的列；
    // 第四个参数是 Aggregator.Op类型的aop，是我们需要使用的Aggregation operator。
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        gbfieldType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        agg = child.getTupleDesc().getFieldType(gfield) == Type.STRING_TYPE ?
                new StringAggregator(gfield, gbfieldType, afield, aop) :
                new IntegerAggregator(gfield, gbfieldType, afield, aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // groupField()：如果这个Aggregate伴随有 groupby，返回groupby的field 的索引。
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// groupFieldName()：如果这个Aggregate伴随有 groupby，返回groupby的field 的Name。
        return gfield == Aggregator.NO_GROUPING ? null : it.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return gfield ==Aggregator.NO_GROUPING ? it.getTupleDesc().getFieldName(0) :
                it.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        child.open();
        super.open();
        while(child.hasNext())
            agg.mergeTupleIntoGroup(child.next());
        it = agg.iterator();
        it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // fetchNext()：返回下一个tuple。如果有groupby field，那么第一个field是我们group的field，
        // 第二个field是计算的aggregate结果；如果没有groupby field，只需要返回结果。
	    return it.hasNext() ? it.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// getTupleDesc()：返回这个aggregate计算结果tuple的TupleDesc。
        Type[] typeArray;
        String[] fieldName;
        if(gfield == Aggregator.NO_GROUPING){
            typeArray = new Type[]{Type.INT_TYPE};
            fieldName = new String[]{child.getTupleDesc().getFieldName(afield)};
        } else {
            typeArray = new Type[]{child.getTupleDesc().getFieldType(gfield),Type.INT_TYPE};
            fieldName = new String[]{child.getTupleDesc().getFieldName(gfield),
                child.getTupleDesc().getFieldName(afield)};
        }
        TupleDesc res = new TupleDesc(typeArray, fieldName);
        return res;
    }

    public void close() {
	// some code goes here
        super.close();
        it.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        child = children[0];
    }
    
}
