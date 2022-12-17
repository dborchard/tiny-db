package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.QueryData;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.C_ProjectPlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectPlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectWithIndexPlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.A_TablePlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.index.IndexInfo;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;

import java.util.Map;

/**
 * The Query Planner without Indexes.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class BetterQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BetterQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }


    public Plan createPlan(QueryData data, Transaction tx) {

        //TODO: issue with SELECT with Index and NonIndex field
        /*
        create table T2 ( A int, B varchar(9) );
        create index A_IDX on T2(A);
        insert into T2 (A, B) values (1, 'Alice');
        insert into T2 (A, B) values (2, 'Bob');
        select A,B from T2;
        select A,B from T2 where A=2 and B='Alice';
        * */
        Plan p = new A_TablePlan(tx, data.table(), mdm);

        boolean indexFound = false;
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(data.table(), tx);
        for (String columnName : indexes.keySet()) {
            D_Constant columnValue = data.pred().equatesWithConstant(columnName);
            if (columnValue != null) {
                IndexInfo columnIndexInfo = indexes.get(columnName);
                p = new B_SelectWithIndexPlan(p, columnIndexInfo, columnValue);

                indexFound = true;
                System.out.println("index on " + columnName + " used");
                break;
            }
        }

        if (!indexFound) p = new B_SelectPlan(p, data.pred());

        p = new C_ProjectPlan(p, data.fields());
        return p;
    }
}
