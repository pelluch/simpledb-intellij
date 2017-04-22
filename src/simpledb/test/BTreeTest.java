package simpledb.test;

import java.util.Iterator;
import java.util.Map;

import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import simpledb.index.Index;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.index.query.IndexSelectPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.IndexMgr;
import simpledb.metadata.TableMgr;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.query.*;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

@RunWith(JUnit4.class)
public class BTreeTest extends TestCase
{

    /** Copyright statement. */
    private static final String COPYRIGHT_NOTICE = "Copyright (c) Pontificia Universidad Catolica de Chile, 2014.";

    private static IndexMgr idxmgr;

    private static TableMgr tblMgr;

    private static Transaction tx;

    private static final String idxname = "index";
    private static final String tblname = "testindextable";
    private static final String fldname = "fld1";
    private static final String serverDirectory = "testdb0";

    private void createDummyDatabase(String tblname, Schema sch)
    {
        tblMgr.createTable(tblname, sch, tx);
    }

    @Before
    public void initializeDatabaseServer()
    {
        Schema sch = new Schema();
        sch.addIntField("id");
        sch.addStringField("fld1", TableMgr.MAX_NAME);
        sch.addStringField("fld2", TableMgr.MAX_NAME);

        SimpleDB.initFileLogAndBufferMgr(serverDirectory);
        tx = new Transaction();
        tblMgr = new TableMgr(true, tx);
        idxmgr = new IndexMgr(true, tblMgr, tx);
        createDummyDatabase(tblname, sch);
        idxmgr.createIndex(idxname, tblname, fldname, tx);

        SimpleDB.initMetadataMgr(true, tx);
    }

    @Test
    public void createIndexTest()
    {
        IndexUpdatePlanner p = new IndexUpdatePlanner();

        String[] commands = new String[] {
                "insert into testindextable(id, fld1, fld2) values (1, 'joe', 'this')",
                "insert into testindextable(id, fld1, fld2) values (2, 'abe', 'is')",
                "insert into testindextable(id, fld1, fld2) values (3, 'adolph', 'just')",
                "insert into testindextable(id, fld1, fld2) values (4, 'alphonse', 'some')",
                "insert into testindextable(id, fld1, fld2) values (5, 'pablo', 'random')",
                "insert into testindextable(id, fld1, fld2) values (6, 'joseph', 'text')",
                "insert into testindextable(id, fld1, fld2) values (7, 'alex', 'because')",
                "insert into testindextable(id, fld1, fld2) values (8, 'ana', 'i')",
                "insert into testindextable(id, fld1, fld2) values (9, 'carol', 'want')",
                "insert into testindextable(id, fld1, fld2) values (10, 'mary', 'some')",
                "insert into testindextable(id, fld1, fld2) values (11, 'gayle', 'quick')",
                "insert into testindextable(id, fld1, fld2) values (12, 'beth', 'examples')"
        };

        for(String s : commands) {
            // System.out.println(s);
            Parser parser = new Parser(s);
            Object obj = parser.updateCmd();
            if (obj instanceof InsertData) {
                p.executeInsert((InsertData) obj, tx);
            } else if(obj instanceof DeleteData) {
                p.executeDelete((DeleteData)obj, tx);
            }

        }

        findWithIndex("fld1", new StringConstant("joseph"), "id");



        /* IndexSelectPlan indexSel = new IndexSelectPlan(
                p,
                idxmgr.getIndexInfo(tblname,
                        tx)
                .get("id"),
                new IntConstant(9),
                tx
        );
        */



        // first, insert the record
        /* UpdateScan scan = (UpdateScan) p.open();
        scan.insert();
        RID rid = scan.getRid();

        // then modify each field, inserting an index record if appropriate
        Map<String, IndexInfo> indexes = SimpleDB.mdMgr().getIndexInfo(tblname, tx);

        Iterator<Constant> valIter = ((InsertData) obj).vals().iterator();
        for (String dataFldname : ((InsertData) obj).fields())
        {
            Constant val = valIter.next();
            System.out.println("Modify field " + dataFldname + " to val " + val);
            scan.setVal(dataFldname, val);

            IndexInfo ii = indexes.get(dataFldname);
            if (ii != null)
            {
                Index idx = ii.open();
                idx.insert(val, rid);
                idx.close();
            }
        }
        scan.close();
        */

        // Estas tres lineas pueden sustituir al codigo de las lineas 79 a 104.
        // Esas lineas son simplemente el metodo executeInsert del
        // IndexUpdatePlanner()
        
        //IndexUpdatePlanner uplanner = new IndexUpdatePlanner();
        //if (obj instanceof InsertData)
        //    uplanner.executeInsert((InsertData)obj, tx);

    }

    private static void deleteWithIndex(String column,
                                        Constant dataVal,
                                        String printColumn) {
        Plan plan = new TablePlan(tblname, tx);
        IndexSelectPlan selectPlan = new IndexSelectPlan(
                plan, idxmgr.getIndexInfo(tblname, tx).get(column),
                dataVal,
                tx
        );
    }

    private static void findWithIndex(String column,
                                      Constant dataVal,
                                      String printColumn) {

        Plan plan = new TablePlan(tblname, tx);
        IndexSelectPlan selectPlan = new IndexSelectPlan(
                plan, idxmgr.getIndexInfo(tblname, tx).get(column),
                dataVal,
                tx
        );
        Scan scan = selectPlan.open();
        scan.beforeFirst();
        while (scan.next()) {
            System.out.println("Found  " + scan.getVal(column) + " - " +
            scan.getVal(printColumn));
        }

        scan.close();
    }

}
