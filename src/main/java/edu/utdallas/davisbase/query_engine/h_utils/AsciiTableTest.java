package edu.utdallas.davisbase.query_engine.h_utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AsciiTableTest {
    public static void main(String[] args) {
        List<String> columnNames = new ArrayList<>();
        columnNames.addAll(Arrays.asList("Name", "Age"));

        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList("Arjun", "26"));
        rows.add(Arrays.asList("Vishnu", "27"));

        new AsciiTable().print(columnNames, rows);
    }

}
