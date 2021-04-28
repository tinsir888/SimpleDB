package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    // JoinPredicate的构造函数：JoinPredicate利用一个Predicate对两个tuples的fields进行比较，JoinPredicate最常被Join operator使用。
    // 构造函数创建在两个tuples的两个fields上创建一个新的Predicate。
    // 有三个参数，第一个参数和第二个参数是field1和field2，是Predicate中第一个tuple和第二个tuple的下标；第三个参数是op，是应用的operation。
    private final int field1;
    private final Predicate.Op op;
    private final int field2;
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // filter(Tuple t1, Tuple t2)：将Predicate用于两个特定tuples。
        return t1.getField(this.field1).compare(this.op, t2.getField(this.field2));
    }
    
    public int getField1()
    {
        // some code goes here
        return this.field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return this.field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here;
        return this.op;
    }
}
