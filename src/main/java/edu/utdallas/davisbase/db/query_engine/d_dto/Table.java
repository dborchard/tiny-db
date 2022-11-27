package edu.utdallas.davisbase.db.query_engine.d_dto;

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
