package edu.utdallas.davisbase.server.c_key_value_store.b_buffer_mgr;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.Page;

public class BufferTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("buffertest", 400, 3); // only 3 buffers
        BufferMgr bm = db.bufferMgr();

        Buffer buff1 = bm.pin(new BlockId("testfile", 1));
        Page p = buff1.contents();
        int n = p.getInt(80);
        p.setInt(80, n + 1);
        buff1.setModified(1, 0); //placeholder values
        System.out.println("The new value is " + (n + 1));
        bm.unpin(buff1);
        // One of these pins will flush buff1 to disk:
        Buffer buff2 = bm.pin(new BlockId("testfile", 2));
        Buffer buff3 = bm.pin(new BlockId("testfile", 3));
        Buffer buff4 = bm.pin(new BlockId("testfile", 4));

        bm.unpin(buff2);
        buff2 = bm.pin(new BlockId("testfile", 1));
        Page p2 = buff2.contents();
        p2.setInt(80, 9999);     // This modification
        buff2.setModified(1, 0); // won't get written to disk.
    }
}
