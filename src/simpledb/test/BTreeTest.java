package simpledb.test;

import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import simpledb.index.Index;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.IndexMgr;
import simpledb.metadata.TableMgr;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
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
    private static final String fldname = "id";
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
        String s = "insert into testindextable(id, fld1, fld2) values (1, 'joe', 'loco') ";
        Parser parser = new Parser(s);
        Object obj = parser.updateCmd();

        IndexUpdatePlanner p = new IndexUpdatePlanner();
        if (obj instanceof InsertData)
            p.executeInsert((InsertData)obj, tx);

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

}
