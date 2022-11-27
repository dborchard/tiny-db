package edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;

public class BufferMgrTest {
    public static void main(String[] args) throws Exception {
        SimpleDB db = new SimpleDB("buffermgrtest", 400, 3); // only 3 buffers
        BufferMgr bm = db.bufferMgr();

        Buffer[] buff = new Buffer[6];
        buff[0] = bm.pin(new BlockId("testfile", 0));
        buff[1] = bm.pin(new BlockId("testfile", 1));
        buff[2] = bm.pin(new BlockId("testfile", 2));
        bm.unpin(buff[1]);
        buff[1] = null;
        buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
        buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
        System.out.println("Available buffers: " + bm.available());
        try {
            System.out.println("Attempting to pin block 3...");
            buff[5] = bm.pin(new BlockId("testfile", 3)); // will not work; no buffers left
        } catch (BufferAbortException e) {
            System.out.println("Exception: No available buffers\n");
        }
        bm.unpin(buff[2]);
        buff[2] = null;
        buff[5] = bm.pin(new BlockId("testfile", 3)); // now this works

        System.out.println("Final Buffer Allocation:");
        for (int i = 0; i < buff.length; i++) {
            Buffer b = buff[i];
            if (b != null)
                System.out.println("buff[" + i + "] pinned to block " + b.block());
        }
    }
}
