package edu.utdallas.davisbase.server.query_engine.d_domain.dto;

import java.util.List;

public class Table {

    public List<String> columnNames;

    public List<List<String>> rowValues;

    public String message;

    public Table(List<String> columnNames, List<List<String>> rowValues) {
        this.columnNames = columnNames;
        this.rowValues = rowValues;
        this.message = "";
    }

    public Table(String message) {
        this.message = message;
    }
}
