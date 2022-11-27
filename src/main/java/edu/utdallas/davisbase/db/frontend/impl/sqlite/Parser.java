package edu.utdallas.davisbase.db.frontend.impl.sqlite;

import edu.utdallas.davisbase.c_parse.domain.commands.*;
import edu.utdallas.davisbase.db.frontend.domain.commands.*;
import edu.utdallas.davisbase.db.query_engine.e_record.Schema;
import edu.utdallas.davisbase.db.frontend.domain.clause.A_Predicate;
import edu.utdallas.davisbase.db.frontend.domain.clause.B_Term;
import edu.utdallas.davisbase.db.frontend.domain.clause.C_Expression;
import edu.utdallas.davisbase.frontend.domain.commands.*;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.query_engine.c_parse.domain.commands.*;

import java.util.ArrayList;
import java.util.List;


public class Parser {
    private Lexer lex;

    public Parser(String s) {
        lex = new Lexer(s);
    }

    public String field() {
        return lex.eatId();
    }

    public D_Constant constant() {
        if (lex.matchStringConstant()) return new D_Constant(lex.eatStringConstant());
        else return new D_Constant(lex.eatIntConstant());
    }

    public C_Expression expression() {
        if (lex.matchId()) return new C_Expression(field());
        else return new C_Expression(constant());
    }

    public B_Term term() {
        C_Expression lhs = expression();
        lex.eatDelim('=');
        C_Expression rhs = expression();
        return new B_Term(lhs, rhs);
    }

    // OK
    public A_Predicate predicate() {
        A_Predicate pred = new A_Predicate(term());
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }


    // OK
    public QueryData queryCmd() {
        lex.eatKeyword("select");
        List<String> fields = selectList();
        lex.eatKeyword("from");
        String table = lex.eatId();
        A_Predicate pred = new A_Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, table, pred);
    }

    // OK
    private List<String> selectList() {
        List<String> L = new ArrayList<String>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(selectList());
        }
        return L;
    }


// Methods for parsing the various update commands

    public Object updateCmd() {
        if (lex.matchKeyword("insert")) return insert();
        else if (lex.matchKeyword("delete")) return delete();
        else if (lex.matchKeyword("update")) return modify();
        else return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table")) return createTable();
        else return createIndex();
    }

// Method for parsing delete commands

    public DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblname = lex.eatId();
        A_Predicate pred = new A_Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblname, pred);
    }

// Methods for parsing insert commands

    public InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<D_Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }

    private List<String> fieldList() {
        List<String> L = new ArrayList<String>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<D_Constant> constList() {
        List<D_Constant> L = new ArrayList<D_Constant>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }

// Method for parsing modify commands

    public ModifyData modify() {
        lex.eatKeyword("update");
        String tblname = lex.eatId();
        lex.eatKeyword("set");
        String fldname = field();
        lex.eatDelim('=');
        C_Expression newval = expression();
        A_Predicate pred = new A_Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblname, fldname, newval, pred);
    }

// Method for parsing create table commands

    public CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldname = field();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldname);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }


//  Method for parsing create index commands

    public CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxname = lex.eatId();
        lex.eatKeyword("on");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        String fldname = field();
        lex.eatDelim(')');
        return new CreateIndexData(idxname, tblname, fldname);
    }
}

