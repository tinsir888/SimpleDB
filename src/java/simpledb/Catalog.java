package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * 目录（SimpleDB中的Catalog类）由以下列表组成：
     * 表和数据库中当前表的模式。 你会
     * 需要支持添加新表以及获取信息的能力
     * 关于一个特定的表。 与每个表相关联的是
     * 一个允许操作员确定类型的TupleDesc对象
     * 和表中的字段数。
     * 全局目录是一个实例
     * 为整个SimpleDB进程分配的目录集。
     * 可以通过方法检索全局目录
     * Database.getCatalog（），同样如此
     * 全局缓冲池（使用Database.getBufferPool（））。
     * Constructor.
     * Creates a new, empty catalog.
     */
    // class Table：为Catalog存储的一个个表建立的辅助类，Table类的构造函数需要三个参数，第一个参数是DbFile类型，是table的内容；第二个参数是String类型，是table 的name；第三个参数是pkeyField，代表表中主键的fieldName。
    private static class Table{
        private static final long serialVersionUID = 1L;

        public final DbFile dbFile;
        public final String tableName;
        public final String pk;

        public Table(DbFile file, String name, String pkeyField){
            dbFile = file;
            tableName = name;
            pk = pkeyField;
        }
        public String toString(){
            return tableName + "(" + dbFile.getId() + ":" + pk + ")";
        }
    }
    private final ConcurrentHashMap<Integer, Table> hashTable;
    public Catalog() {
        // Catalog构造函数：创建一个<Interger,Table>的哈希表，用于存储已经实例化的表。
        hashTable = new ConcurrentHashMap<Integer, Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // addTable(DbFile file, String name, String pkeyField)：在哈希表中添加一个Table。
        Table t = new Table(file, name, pkeyField);
        hashTable.put(file.getId(), t);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // getTableId(String name)：遍历哈希表中的Tables，找到对应名字返回table的Id。
        Integer id = hashTable.searchValues(1, value -> {
            if(value.tableName.equals(name)){return value.dbFile.getId();}
            else return null;
        });
        if(id != null){
            return id.intValue();
        } else {
            throw new NoSuchElementException("Not found id for table " + name);
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // getTupleDesc(int tableid)：返回tableid表对应的TupleDesc表结构。
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.dbFile.getTupleDesc();
        } else {
            throw new NoSuchElementException("Not found tuple desc for table " + tableid);
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // getDatabaseFile(int tableid)：返回tableid表对应的表数据DbFile。
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.dbFile;
        } else{
            throw new NoSuchElementException("Not found DBfile for table " + tableid);
        }
    }

    public String getPrimaryKey(int tableid) {
        // getPrimaryKey(int tableid)：返回tableid表对应的主键名。
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.pk;
        } else{
            throw new NoSuchElementException("Not found primary key for table " + tableid);
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().iterator();
    }

    public String getTableName(int tableid) {
        // getTableName(int id)：返回tableid表对应的TableName。
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.tableName;
        } else{
            throw new NoSuchElementException("Not found name for table " + tableid);
        }
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // clear()：从Catalog中删除所有的tables。
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        //loadSchema(String catalogFile)：利用正则化从file中读取表的结构，并在数据库中创建所有合适的表。
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

