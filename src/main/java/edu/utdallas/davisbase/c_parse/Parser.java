package edu.utdallas.davisbase.c_parse;

import edu.utdallas.davisbase.c_parse.commands.*;
import edu.utdallas.davisbase.e_record.Schema;
import edu.utdallas.davisbase.d_scans.domains.Constant;
import edu.utdallas.davisbase.d_scans.domains.Expression;
import edu.utdallas.davisbase.d_scans.domains.Predicate;
import edu.utdallas.davisbase.d_scans.domains.Term;

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

    public Constant constant() {
        if (lex.matchStringConstant()) return new Constant(lex.eatStringConstant());
        else return new Constant(lex.eatIntConstant());
    }

    public Expression expression() {
        if (lex.matchId()) return new Expression(field());
        else return new Expression(constant());
    }

    public Term term() {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    // OK
    public Predicate predicate() {
        Predicate pred = new Predicate(term());
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
        Predicate pred = new Predicate();
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
        Predicate pred = new Predicate();
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
        List<Constant> vals = constList();
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

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<Constant>();
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
        Expression newval = expression();
        Predicate pred = new Predicate();
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

