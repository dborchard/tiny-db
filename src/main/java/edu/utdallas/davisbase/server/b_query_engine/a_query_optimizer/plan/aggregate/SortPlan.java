package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils.RecordComparator;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils.TempTable;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.aggregate.SortScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.UpdateScan;

import java.util.ArrayList;
import java.util.List;

/**
 * The Plan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private RecordValueSchema sch;
    private RecordComparator comp;

    /**
     * Create a sort plan for the specified query.
     *
     * @param p          the plan for the underlying query
     * @param sortfields the fields to sort by
     * @param tx         the calling transaction
     */
    public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
        this.tx = tx;
        this.p = p;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
    }

    /**
     * This method is where most of the action is.
     * Up to 2 sorted temporary tables are created,
     * and are passed into SortScan for final merging.
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2) runs = doAMergeIteration(runs);
        return new SortScan(runs, comp);
    }

    /**
     * Return the number of blocks in the sorted table,
     * which is the same as it would be in a
     * materialized table.
     * It does <i>not</i> include the one-time cost
     * of materializing and sorting the records.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Return the number of records in the sorted table,
     * which is the same as in the underlying query.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Return the number of distinct field values in
     * the sorted table, which is the same as in
     * the underlying query.
     *
     * @see simpledb.plan.Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Return the schema of the sorted table, which
     * is the same as in the underlying query.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public RecordValueSchema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.seekToHead_Query();
        if (!src.next()) return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) if (comp.compare(src, currentscan) < 0) {
            // start a new run
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
        }
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) result.add(runs.get(0));
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        while (hasmore1 && hasmore2) if (comp.compare(src1, src2) < 0) hasmore1 = copy(src1, dest);
        else hasmore2 = copy(src2, dest);

        if (hasmore1) while (hasmore1) hasmore1 = copy(src1, dest);
        else while (hasmore2) hasmore2 = copy(src2, dest);
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.seekToHead_Insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }
}
