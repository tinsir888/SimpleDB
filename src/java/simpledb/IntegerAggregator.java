package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    static private final Field NULL_HASH_KEY=new IntField(2147483647);
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc rem;
    private ConcurrentHashMap<Field,items> res;
    private String[] fieldName;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        res=new ConcurrentHashMap<>();
        fieldName=new String[2];
    }

    public class items{
        public int val;
        public int ave;
        public int sum;
        public items(int val,int ave){
            this.val=val; this.ave=ave; sum=0;
        }
        public items(int val){
            this.val=val; this.ave=0; sum=0;
        }
        public items(){
            this.val=0; this.ave=0; sum=0;
        }
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup)
            throws UnsupportedOperationException{
        // some code goes here
        Field hash_gbfield;
        if(gbfield!=Aggregator.NO_GROUPING) hash_gbfield=tup.getField(gbfield);
        else hash_gbfield=NULL_HASH_KEY;

        IntField field_to_aggregate=(IntField) tup.getField(afield);
        int num_to_aggregate=field_to_aggregate.getValue();

        if(gbfield==Aggregator.NO_GROUPING) fieldName[0]=null;
        else fieldName[0]=tup.getTupleDesc().getFieldName(gbfield);
        fieldName[1]=tup.getTupleDesc().getFieldName(afield);

        if(tup.getField(afield).getType()!=Type.INT_TYPE)
            throw new UnsupportedOperationException("not INT_TYPE");

        if(!res.containsKey(hash_gbfield)){
            items newItem=new items(num_to_aggregate,0);
            if(what==Op.AVG){
                newItem.ave=1;
                newItem.sum=num_to_aggregate;
            }
            else if(what==Op.COUNT){
                newItem.val=1;
            }
            res.put(hash_gbfield,newItem);
        }
        else{
            items newItem=res.get(hash_gbfield);
            if(what==Op.AVG){
                newItem.sum=newItem.sum+num_to_aggregate;
                newItem.ave++;
                newItem.val=newItem.sum/newItem.ave;
            }
            else{
                newItem.val=update(newItem.val,num_to_aggregate,what);
            }
            res.replace(hash_gbfield,newItem);
        }
    }

    private int update(int val,int pre,Op op)
            throws UnsupportedOperationException{
        switch (op) {
            case MAX:
                return Math.max(val,pre);
            case MIN:
                return Math.min(val,pre);
            case SUM:
                return val+pre;
            case COUNT:
                return val+1;
            default:
                throw new UnsupportedOperationException("AVG is not here");
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregateIterator();
    }
    class IntegerAggregateIterator implements OpIterator{
        private ArrayList<Tuple> remTu;
        private Iterator<Tuple> it;
        public IntegerAggregateIterator() {
            remTu = new ArrayList<>();
            for (ConcurrentHashMap.Entry<Field, items> it : res.entrySet()) {
                Type[] fieldType;
                String[] fieldNameAr;
                if(gbfield==Aggregator.NO_GROUPING){
                    fieldType=new Type[]{Type.INT_TYPE};
                    fieldNameAr=new String[]{fieldName[1]};
                }
                else{
                    fieldType=new Type[]{gbfieldtype,Type.INT_TYPE};
                    fieldNameAr=new String[]{fieldName[0],fieldName[1]};
                }
                rem=new TupleDesc(fieldType,fieldNameAr);
                Tuple t = new Tuple(rem);

                if (gbfield == Aggregator.NO_GROUPING){
                    t.setField(0, new IntField(it.getValue().val));
                }
                else{
                    t.setField(0, it.getKey());
                    t.setField(1, new IntField(it.getValue().val));
                }
                remTu.add(t);
            }
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            it=remTu.iterator();
        }
        @Override
        public boolean hasNext() throws DbException,TransactionAbortedException{
            return it.hasNext();
        }
        @Override
        public Tuple next(){
            return it.next();
        }
        @Override
        public void rewind() throws DbException,TransactionAbortedException{
            this.close();
            this.open();
        }
        @Override
        public TupleDesc getTupleDesc(){
            return rem;
        }
        @Override
        public void close(){
            it=null;
        }
    }
}
