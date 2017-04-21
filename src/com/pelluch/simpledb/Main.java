package com.pelluch.simpledb;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.query.*;
import simpledb.record.RID;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Main {

    private static Connection conn = null;

    private static void doQuery(String cmd) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            ResultSetMetaData md = rs.getMetaData();
            int numcols = md.getColumnCount();
            int totalwidth = 0;

            // print header
            for(int i=1; i<=numcols; i++) {
                int width = md.getColumnDisplaySize(i);
                totalwidth += width;
                String fmt = "%" + width + "s";
                System.out.format(fmt, md.getColumnName(i));
            }
            System.out.println();
            for(int i=0; i<totalwidth; i++)
                System.out.print("-");
            System.out.println();

            // print records
            while(rs.next()) {
                for (int i=1; i<=numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int fldtype = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fldtype == Types.INTEGER)
                        System.out.format(fmt + "d", rs.getInt(fldname));
                    else
                        System.out.format(fmt + "s", rs.getString(fldname));
                }
                System.out.println();
            }
            rs.close();
        }
        catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void doUpdate(String cmd) {
        try {
            Statement stmt = conn.createStatement();
            int howmany = stmt.executeUpdate(cmd);
            System.out.println(howmany + " records processed");
        }
        catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        Driver d = new SimpleDriver();
        try {
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();
            long startTime = System.nanoTime();
            for(int i = 1; i < 1000; ++i) {
                String s = "select SName from STUDENT where SId = " + i;
                stmt.executeQuery(s);
            }
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            System.out.println("Query time: " + duration);


            /* String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
            stmt.executeUpdate(s);
            s = "create index sididx on STUDENT(SId)";
            stmt.executeUpdate(s);

            */

            SimpleDB.init("test");
            Transaction tx = new Transaction();
            Plan studentPlan = new TablePlan("student", tx);
            TableScan studentScan = (TableScan) studentPlan.open();

            MetadataMgr mdmgr = SimpleDB.mdMgr();
            Map<String, Index> indexes = new HashMap<>();
            Map<String, IndexInfo> idxInfo = mdmgr.getIndexInfo("student", tx);
            for (String fieldName : idxInfo.keySet()) {
                if(fieldName.equals("sid")) {
                    Index idx = idxInfo.get(fieldName).open();
                    indexes.put(fieldName, idx);
                }
            }

            /* Random random = new Random();

            for(int i = 1; i < 1000; ++i) {
                System.out.println("Doing: " + i);
                addStudent(studentScan, i, "joe " + i, 2010, 30);
                insertIntoIndex(studentScan, indexes);
            }
            */

            IndexInfo ii = idxInfo.get("sid");
            startTime = System.nanoTime();
            for(int i = 1; i < 1000; ++i) {
                findStudent(studentScan, ii, "sid", new IntConstant(i));
            }
            endTime = System.nanoTime();
            duration = (endTime - startTime) / 1000000;
            System.out.println("Query time: " + duration);

            studentScan.close();
            tx.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void findStudent(TableScan studentScan, IndexInfo ii, String column, Constant dataVal) {
        Index idx = ii.open();
        idx.beforeFirst(dataVal);
        while(idx.next()) {
            RID dataRid = idx.getDataRid();
            studentScan.moveToRid(dataRid);
            // System.out.println(studentScan.getString("sname"));
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
