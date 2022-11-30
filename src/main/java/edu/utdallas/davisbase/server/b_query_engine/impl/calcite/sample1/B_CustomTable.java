package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.sample1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class B_CustomTable extends AbstractTable implements ScannableTable {

    private final Map<Object, ObjectNode> data;

    private final List<String> fieldNames;

    private final List<SqlTypeName> fieldTypes;

    public B_CustomTable(Map<Object, ObjectNode> data) {

        this.data = data;

        List<String> names = new ArrayList<>();
        names.add("id");
        names.add("name");
        names.add("age");
        this.fieldNames = names;

        List<SqlTypeName> types = new ArrayList<>();
        types.add(SqlTypeName.BIGINT);
        types.add(SqlTypeName.VARCHAR);
        types.add(SqlTypeName.INTEGER);
        this.fieldTypes = types;
    }

    /**
     * Mapping FieldType+FieldName as RelDataType
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataType> types = fieldTypes.stream().map(typeFactory::createSqlType).collect(Collectors.toList());
        return typeFactory.createStructType(types, fieldNames);
    }

    /**
     * Convert data entries in Linq Enumerable. Required for row by row iteration.
     * <p>
     * Uses the below helper for String to Object conversion.
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        Stream<Object[]> dataStream = data.entrySet().stream().map(this::toObjectArray);
        return Linq4j.asEnumerable(new StreamIterable<>(dataStream));
    }

    /**
     * <code>
     * For each row {Key: Object (PrimaryKey Column), Value:JSONObjectNode},
     * convert to result[n] where n=columnCount.
     *
     * <p>
     * n = 1 PrimaryKeyColumn + N-1 remaining columns.
     *
     * <p>
     * result[0] = row.getPrimaryKeyColumnValue()
     * result[1] = row.getValue().getJSONNode( columnName[1] ).respectiveValue(int or str)
     * ....
     *
     * </code>
     */

    private Object[] toObjectArray(Map.Entry<Object, ObjectNode> row) {

        Object[] result = new Object[fieldNames.size()];
        result[0] = row.getKey();

        for (int i = 1; i < fieldNames.size(); i++) {
            JsonNode v = row.getValue().get(fieldNames.get(i));
            SqlTypeName type = fieldTypes.get(i);
            switch (type) {
                case VARCHAR:
                    result[i] = v.textValue();
                    break;
                case INTEGER:
                    result[i] = v.intValue();
                    break;
                default:
                    throw new RuntimeException("unsupported sql type: " + type);
            }
        }
        return result;
    }

    private static class StreamIterable<T> implements Iterable<T> {

        private final Stream<T> stream;

        StreamIterable(Stream<T> stream) {
            this.stream = stream;
        }

        @Override
        public Iterator<T> iterator() {
            return stream.iterator();
        }

    }
}