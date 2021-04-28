package simpledb;

import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.IOException;

public class DatFileViewTest extends SimpleDbTestBase {
    @Test
    public void test() throws IOException, TransactionAbortedException, DbException {
        HeapFile heapFile = SystemTestUtil.createRandomHeapFile(10, 300, 10, null, null);
        System.out.println(heapFile.getFile().getAbsolutePath());
    }
}
