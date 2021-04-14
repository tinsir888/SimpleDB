package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private ConcurrentHashMap<Field, Integer> res;
    private TupleDesc rem;
    private String[] fieldName;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        res = new ConcurrentHashMap<>();
        fieldName = new String[2];
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field hash_gbfield = tup.getField(gbfield);
        Field hash_afield = tup.getField(afield);
        /*if(hash_afield.getType() != Type.STRING_TYPE){
            Field teemp = hash_afield;
            hash_afield = hash_gbfield;
            hash_gbfield = teemp;
            throw new IllegalArgumentException("afield is not string");
        }*/
        if(gbfield == Aggregator.NO_GROUPING) fieldName[0] = null;
        else fieldName[0] = tup.getTupleDesc().getFieldName(gbfield);
        fieldName[1] = tup.getTupleDesc().getFieldName(afield);
        //exception:afield is not a string type
        /*if(tup.getField(afield).getType() != Type.STRING_TYPE){
            hash_gbfield = tup.getField(afield);
        }*/
        /*
        if(tup.getField(gbfield).getType() != Type.STRING_TYPE)
            throw new IllegalArgumentException("not a STRING_TYPE");
        */
        if(!res.containsKey(hash_gbfield)) {
            if (what != Op.COUNT && what != Op.SUM)
                throw new IllegalArgumentException("Operator not supported!");
            if (what == Op.COUNT){
                Integer newItem = 1;
                res.put(hash_gbfield, newItem);
            } else {
                Integer newItem = ((IntField)hash_afield).getValue()/**/;
                res.put(hash_gbfield, newItem);
            }
        } else{
            Integer newItem = res.get(hash_gbfield);
            newItem += what == Op.COUNT ? 1 : ((IntField)hash_afield).getValue()/**/;
            res.put(hash_gbfield, newItem);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregateIterator();
    }
    class StringAggregateIterator implements OpIterator{
        private ArrayList<Tuple> trem;
        private Iterator<Tuple> it;
        public StringAggregateIterator(){
            trem = new ArrayList<>();
            for(ConcurrentHashMap.Entry<Field, Integer> it : res.entrySet()){
                Type[]  typeArray;
                String[] fieldArray;
                if(gbfield == Aggregator.NO_GROUPING){
                    typeArray = new Type[]{Type.INT_TYPE};
                    fieldArray = new String[]{fieldName[1]};
                } else {
                    typeArray = new Type[]{gbfieldtype, Type.INT_TYPE};
                    fieldArray = new String[]{fieldName[0], fieldName[1]};
                }
                rem = new TupleDesc(typeArray, fieldArray);
                Tuple tmp = new Tuple(rem);

                if(gbfield == Aggregator.NO_GROUPING){
                    tmp.setField(0, new IntField(it.getValue()));
                    System.out.println(tmp.getField(0));
                } else {
                    tmp.setField(0, it.getKey());
                    tmp.setField(1, new IntField(it.getValue()));
                }
                trem.add(tmp);
            }
        }
        @Override
        public void open(){
            it = trem.iterator();
        }
        @Override
        public boolean hasNext(){
            return it.hasNext();
        }
        @Override
        public Tuple next(){
            return it.next();
        }
        @Override
        public void rewind(){
            this.close();
            this.open();
        }
        @Override
        public TupleDesc getTupleDesc(){
            return rem;
        }
        @Override
        public void close(){
            it = null;
        }
    }
}
