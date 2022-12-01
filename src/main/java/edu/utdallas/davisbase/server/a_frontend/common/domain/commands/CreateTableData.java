package edu.utdallas.davisbase.server.a_frontend.common.domain.commands;

import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.TableDefinition;
import lombok.ToString;

/**
 * Data for the SQL <i>create table</i> statement.
 *
 * @author Edward Sciore
 */
@ToString
public class CreateTableData {
    private String tblname;
    private TableDefinition sch;

    /**
     * Saves the table name and schema.
     */
    public CreateTableData(String tblname, TableDefinition sch) {
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
    public TableDefinition newSchema() {
        return sch;
    }
}

