package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.aggregate;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils.RecordComparator;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils.TempTable;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.UpdateScan;

import java.util.Arrays;
import java.util.List;

/**
 * The Scan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */

/**
 * @author sciore
 */
public class SortScan implements Scan {
    private UpdateScan s1, s2 = null, currentscan = null;
    private RecordComparator comp;
    private boolean hasmore1, hasmore2 = false;
    private List<RecordKey> savedposition;

    /**
     * Create a sort scan, given a list of 1 or 2 runs.
     * If there is only 1 run, then s2 will be null and
     * hasmore2 will be false.
     *
     * @param runs the list of runs
     * @param comp the record comparator
     */
    public SortScan(List<TempTable> runs, RecordComparator comp) {
        this.comp = comp;
        s1 = (UpdateScan) runs.get(0).open();
        hasmore1 = s1.next();
        if (runs.size() > 1) {
            s2 = (UpdateScan) runs.get(1).open();
            hasmore2 = s2.next();
        }
    }

    /**
     * Position the scan before the first record in sorted order.
     * Internally, it moves to the first record of each underlying scan.
     * The variable currentscan is set to null, indicating that there is
     * no current scan.
     *
     */
    public void seekToHead_Query() {
        currentscan = null;
        s1.seekToHead_Query();
        hasmore1 = s1.next();
        if (s2 != null) {
            s2.seekToHead_Query();
            hasmore2 = s2.next();
        }
    }

    /**
     * Move to the next record in sorted order.
     * First, the current scan is moved to the next record.
     * Then the lowest record of the two scans is found, and that
     * scan is chosen to be the new current scan.
     *
     */
    public boolean next() {
        if (currentscan != null) {
            if (currentscan == s1)
                hasmore1 = s1.next();
            else if (currentscan == s2)
                hasmore2 = s2.next();
        }

        if (!hasmore1 && !hasmore2)
            return false;
        else if (hasmore1 && hasmore2) {
            if (comp.compare(s1, s2) < 0)
                currentscan = s1;
            else
                currentscan = s2;
        } else if (hasmore1)
            currentscan = s1;
        else if (hasmore2)
            currentscan = s2;
        return true;
    }

    /**
     * Close the two underlying scans.
     *
     */
    public void close() {
        s1.close();
        if (s2 != null)
            s2.close();
    }

    /**
     * Get the Constant value of the specified field
     * of the current scan.
     *
     */
    public D_Constant getVal(String fldname) {
        return currentscan.getVal(fldname);
    }

    /**
     * Get the integer value of the specified field
     * of the current scan.
     *
     */
    public int getInt(String fldname) {
        return currentscan.getInt(fldname);
    }

    /**
     * Get the string value of the specified field
     * of the current scan.
     *
     */
    public String getString(String fldname) {
        return currentscan.getString(fldname);
    }

    /**
     * Return true if the specified field is in the current scan.
     *
     */
    public boolean hasField(String fldname) {
        return currentscan.hasField(fldname);
    }

    /**
     * Save the position of the current record,
     * so that it can be restored at a later time.
     */
    public void savePosition() {
        RecordKey rid1 = s1.getRid();
        RecordKey rid2 = (s2 == null) ? null : s2.getRid();
        savedposition = Arrays.asList(rid1, rid2);
    }

    /**
     * Move the scan to its previously-saved position.
     */
    public void restorePosition() {
        RecordKey rid1 = savedposition.get(0);
        RecordKey rid2 = savedposition.get(1);
        s1.moveToRid(rid1);
        if (rid2 != null)
            s2.moveToRid(rid2);
    }
}
