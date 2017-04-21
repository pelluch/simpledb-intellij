package com.pelluch.simpledb;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.query.*;
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

        addStudent(studentScan, 1, "joe", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 2, "amy", 2011, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 3, "sue", 2012, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 4, "bob", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 5, "kim", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 6, "art", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 7, "pat", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 8, "sam", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 9, "joe", 2010, 30);
        insertIntoIndex(studentScan, indexes);
        addStudent(studentScan, 10, "sam", 2010, 30);
        insertIntoIndex(studentScan, indexes);



        IndexInfo ii = idxInfo.get("sname");
        findStudent(studentScan, ii, "sname", new StringConstant("sam"));
        studentScan.close();
        tx.commit();
    }

    private static void findStudent(TableScan studentScan, IndexInfo ii, String column, Constant dataVal) {
        Index idx = ii.open();
        idx.beforeFirst(dataVal);
        while(idx.next()) {
            RID dataRid = idx.getDataRid();
            studentScan.moveToRid(dataRid);
            System.out.println(studentScan.getString("sname"));
        }
        idx.close();
    }
    private static void insertIntoIndex(TableScan studentScan, Map<String, Index> indexes) {
        RID datarid = studentScan.getRid();
        for (String fieldName : indexes.keySet()) {
            Constant dataVal = studentScan.getVal(fieldName);
            Index idx = indexes.get(fieldName);
            idx.insert(dataVal, datarid);
        }
    }

    private static void addStudent(TableScan studentScan, int sid, String name, int year, int majorId) {
        studentScan.insert();
        studentScan.setInt("sid", sid);
        studentScan.setString("sname", name);
        studentScan.setInt("gradyear", year);
        studentScan.setInt("majorid", majorId);
    }
}
