package edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization;

import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;

public interface IStorageEngine {
    // CRUD
    public int getInt(int slot, String fldname);

    public String getString(int slot, String fldname);

    public void setInt(int slot, String fldname, int val);

    public void setString(int slot, String fldname, String val);

    public void delete(int slot);

    // Iterator
    public int nextAfter(int slot);

    public int insertAfter(int slot);

    public BlockId getBlockId();

}