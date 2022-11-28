package edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.b_index;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.heap.RecordKey;

/**
 * This interface contains methods to traverse an index.
 *
 * @author Edward Sciore
 */
public interface Index {

    // CRUD
    public void insert(D_Constant key, RecordKey value);

    public void delete(D_Constant key, RecordKey value);

    // Iterator
    public void seek(D_Constant key);

    public boolean next();

    public void close();

    public RecordKey getRecordId();
}
