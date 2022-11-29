package edu.utdallas.davisbase.server.b_query_engine.common;

import java.util.List;

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