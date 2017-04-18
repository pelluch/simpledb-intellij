package com.pelluch.simpledb;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        SimpleDB.init("test");
        Transaction tx = new Transaction();
        Plan studentPlan = new TablePlan("student", tx);
        TableScan studentScan = (TableScan) studentPlan.open();

        MetadataMgr mdmgr = SimpleDB.mdMgr();
        Map<String, Index> indexes = new HashMap<>();
        Map<String, IndexInfo> idxInfo = mdmgr.getIndexInfo("student", tx);

        for (String fieldName : idxInfo.keySet()) {
            Index idx = idxInfo.get(fieldName).open();
            indexes.put(fieldName, idx);
        }

        // Task 1 : insert a new STUDENT record for SAM
        studentScan.insert();
        studentScan.setInt("sid", 11);
        studentScan.setString("sname", "sam");
        studentScan.setInt("gradyear", 2010);
        studentScan.setInt("majorid", 30);

        RID datarid = studentScan.getRid();
        for (String fieldName : indexes.keySet()) {
            Constant dataVal = studentScan.getVal(fieldName);
            Index idx = indexes.get(fieldName);
            idx.insert(dataVal, datarid);
        }

        studentScan.beforeFirst();
        while ( studentScan.next()) {
            if(studentScan.getString("sname").equals("joe")) {
                RID joeRid = studentScan.getRid();
                for ( String fieldName : indexes.keySet()) {
                    Constant dataVal = studentScan.getVal(fieldName);
                    Index idx = indexes.get(fieldName);
                    idx.delete(dataVal, datarid);
                }
                studentScan.delete();
                break;
            }
        }
        studentScan.close();
        for ( Index idx : indexes.values()) {
            idx.close();
        }
        tx.commit();
    }
}
