package edu.utdallas.davisbase.db.storage_engine.a_io.data;

import edu.utdallas.davisbase.db.storage_engine.d_file.BlockId;

public interface TablePage {
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
