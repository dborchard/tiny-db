package edu.utdallas.davisbase.server.a_frontend.common.domain.commands;

import edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.heap.RecordValueSchema;

/**
 * Data for the SQL <i>create table</i> statement.
 *
 * @author Edward Sciore
 */
public class CreateTableData {
    private String tblname;
    private RecordValueSchema sch;

    /**
     * Saves the table name and schema.
     */
    public CreateTableData(String tblname, RecordValueSchema sch) {
        this.tblname = tblname;
        this.sch = sch;
    }

    /**
     * Returns the name of the new table.
     *
     * @return the name of the new table
     */
    public String tableName() {
        return tblname;
    }

    /**
     * Returns the schema of the new table.
     *
     * @return the schema of the new table
     */
    public RecordValueSchema newSchema() {
        return sch;
    }
}

