package edu.utdallas.tiny_db.server.b_query_engine.common.dto;

import java.util.List;

/**
 * The DTO that is returned to the CLI Driver.
 *
 * @author Arjun Sunil Kumar
 */
public class TableDto {

    public List<String> columnNames;

    public List<List<String>> rowValues;

    public String message;

    public TableDto(List<String> columnNames, List<List<String>> rowValues) {
        this.columnNames = columnNames;
        this.rowValues = rowValues;
        this.message = "";
    }

    public TableDto(String message) {
        this.message = message;
    }
}
