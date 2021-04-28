package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * SimpleDB中的元组是非常基本的。 它们包含以下内容的集合：
 * “字段”对象，在“元组”中每个字段一个。
 * “字段”是具有不同数据类型（例如，
 * 整数，字符串）工具。 “ Tuple”对象是由
 * 基础访问方法（例如，堆文件或B树），如
 * 下一节。 元组也有一个类型（或模式），称为_tuple
 * 描述符_，由“ TupleDesc”对象表示。 这
 * 对象由“类型”对象的集合组成，每个字段一个
 * 在元组中，每个元组描述相应字段的类型。
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private RecordId recordId;
    private final Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        // Tuple构造函数：创建fields数组。
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // getTupleDesc()：返回当前tuple的TupleDesc结构。
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     *         getRecordId()、setRecordId()：获得/设置当前tuple的RecordId，RecordId代表了当前tuple在disk上的位置。
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        // setField(int i, Field f)：为fields数组下标i处的field赋新值。
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        // getField(int i)：获得fields数组下标i处的field值。
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        StringBuilder tmp = new StringBuilder();
        for(int i = 0; i < tupleDesc.numFields() - 1; i ++){
            tmp.append(fields[i].toString()+" ");
        }
        tmp.append(fields[tupleDesc.numFields() - 1].toString() + "\n");
        return tmp.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        // fields()：返回一个迭代器，迭代此tuple内fields数组的所有元素。
        return (Iterator<Field>) Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleDesc = td;
    }
}
