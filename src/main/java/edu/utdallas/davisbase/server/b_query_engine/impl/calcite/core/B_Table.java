package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core;

import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class B_Table extends AbstractTable implements ScannableTable {//, ModifiableTable {

  private final String tableName;
  private final List<String> fieldNames;
  private final List<SqlTypeName> fieldTypes;

  private final Transaction tx;
  private final MetadataMgr mdm;

  private RelDataType rowType;

  public B_Table(String tableName, List<String> fieldNames, List<SqlTypeName> fieldTypes,
      Transaction tx, MetadataMgr mdm
  ) {
    this.tableName = tableName;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;

    this.tx = tx;
    this.mdm = mdm;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

      for (int i = 0; i < fieldNames.size(); i++) {
        RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
        RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
        fields.add(field);
      }

      rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
    }

    return rowType;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new A_Enumerator(mdm, tx, tableName);
      }
    };
  }


}