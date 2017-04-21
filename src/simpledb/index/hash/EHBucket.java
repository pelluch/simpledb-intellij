package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.metadata.IndexInfo;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pablo on 4/21/17.
 */
public class EHBucket {
    private TableInfo ti;
    private Transaction tx;
    private Constant searchkey;
    private EHBucketPage contents;
    private int currentslot = -1;

    EHBucket(Block blk, TableInfo ti, Constant searchkey, Transaction tx) {
        this.ti = ti;
        this.tx = tx;
        this.searchkey = searchkey;
        contents = new EHBucketPage(blk, ti, tx);
        if(searchkey != null) {
            currentslot = contents.findSlotBefore(searchkey);
        }
    }

    int getLocalDepth() {
        return contents.getLocalDepth();
    }

    int increaseLocalDepth() {
        return contents.increaseLocalDepth();
    }
    public void close() {
        contents.close();
    }

    public boolean next() {
        ++currentslot;
        int numRecs = contents.getNumRecs();
        while(currentslot < numRecs) {
            if (contents.getDataVal(currentslot).equals(searchkey)) {
                return true;
            }
            ++currentslot;
        }
        return false;
        /*
        if (currentslot >= contents.getNumRecs())
            return false;
            // return tryOverflow();
        else if (contents.getDataVal(currentslot).equals(searchkey))
            return true;
        else
            throw new IllegalStateException("Bucket is full");
            // return tryOverflow();
            */
    }

    public boolean insert(RID dataid, Constant searchkey) {
        this.searchkey = searchkey;
        if(contents.isFull()) {
            return false;
        }
        currentslot = contents.getNumRecs();
        contents.insertBucket(currentslot, searchkey, dataid);
        // System.out.println("Contents after inserting: ");
        // contents.printAll();
        return true;
    }

    public boolean insert(RID dataid) {
        return insert(dataid, searchkey);
    }

    void print() {
        contents.printAll();
    }

    void redistribute(EHBucket newBucket) {
        List<IndexEntry> originalEntries = new ArrayList<>();
        List<IndexEntry> newEntries = new ArrayList<>();
        int localDepth = getLocalDepth();

        System.out.println("Original entries: ");
        print();

        for(int i = 0; i < contents.getNumRecs(); ++i) {
            IndexEntry entry = new IndexEntry();
            entry.rid = contents.getDataRid(i);
            entry.dataVal = contents.getDataVal(i);
            int bitValue = (entry.dataVal.hashCode() >> localDepth - 1) & 1;
            if(bitValue == 0) {
                originalEntries.add(entry);
            } else {
                newEntries.add(entry);
            }

        }

        if(contents.getNumRecs() == originalEntries.size()) {
            throw new IllegalStateException("All elements in bucket are equal");
        }

        contents.setNumRecs(0);
        for (IndexEntry originalEntry : originalEntries) {
            insert(originalEntry.rid,
                    originalEntry.dataVal);
        }
        newBucket.contents.setNumRecs(0);
        for (IndexEntry newEntry : newEntries) {
            newBucket.insert(newEntry.rid,
                    newEntry.dataVal);
        }

        /* System.out.println("Old bucket entries: ");
        print();
        System.out.println("New bucket entries: ");
        newBucket.print();
        */
        close();
        newBucket.close();
    }

    public RID getDataRid() {
        return contents.getDataRid(currentslot);
    }

    private class IndexEntry {
        RID rid;
        Constant dataVal;
    }
}
