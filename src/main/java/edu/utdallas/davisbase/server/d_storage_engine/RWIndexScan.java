package edu.utdallas.davisbase.server.d_storage_engine;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;

/**
 * This interface contains methods to traverse an index.
 *
 * @author Edward Sciore,Arjun Sunil Kumar
 */
public interface RWIndexScan {

    // CRUD
    public void insert(D_Constant key, RecordKey value);

    public void delete(D_Constant key, RecordKey value);

    // Iterator
    public void seek(D_Constant key);

    public boolean hasNext();

    public RecordKey next();


    public void close();


}
