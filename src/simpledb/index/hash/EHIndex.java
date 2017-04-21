package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.*;
import simpledb.tx.Transaction;

public class EHIndex implements Index {

    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;
    private TableInfo dirInfo;
    private TableInfo bucketInfo;
    private EHBucket bucket = null;

    private EHDir dir;

    /**
     * Opens a hash index for the specified index.
     * @param idxname the name of the index
     * @param sch the schema of the index records
     * @param tx the calling transaction
     */
    public EHIndex(String idxname, Schema sch, Transaction tx) {
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;
        String bucketTable = idxname + "bucket";
        bucketInfo = new TableInfo(bucketTable, sch);
        if(tx.size(bucketInfo.fileName()) == 0) {
            tx.append(bucketInfo.fileName(), new
                    EHBucketPageFormatter(bucketInfo, 0));
        }

        dir = new EHDir(idxname, tx);
        dir.close();
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchkey = searchkey;
        int numBucket = dir.getBucketForKey(searchkey);
        Block bucketBlock = new Block(bucketInfo.fileName(), numBucket);
        bucket = new EHBucket(bucketBlock, bucketInfo, searchkey, tx);
    }

    @Override
    public boolean next() {
        return bucket.next();
    }

    @Override
    public RID getDataRid() {
        return bucket.getDataRid();
    }

    @Override
    public void insert(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        boolean success = bucket.insert(datarid);
        if(!success) {
            int localDepth = bucket.getLocalDepth();
            if(localDepth < dir.getGlobalDepth()) {
                int newDepth = bucket.increaseLocalDepth();
                Block newBlock = tx.append(bucketInfo.fileName(),
                        new EHBucketPageFormatter(bucketInfo, newDepth));
                EHBucket newBucket = new EHBucket(newBlock, bucketInfo, dataval, tx);
                bucket.redistribute(newBucket);
                // Redistribute records
                // Update directory

                dir.updateEntries(dataval, localDepth, newBlock.number());
            } else {
                dir.increaseGlobalDepth();
            }
            insert(dataval, datarid);
        }
    }


    @Override
    public void delete(Constant dataval, RID datarid) {

    }

    @Override
    public void close() {
        if(bucket != null) {
            bucket.close();
            bucket = null;
        }
    }
    /**
     * Returns the cost of searching an index file having the
     * specified number of blocks.
     * The method assumes that all buckets are about the
     * same size, and so the cost is simply the size of
     * the bucket.
     * @param numblocks the number of blocks of index records
     * @param rpb the number of records per block (not used here)
     * @return the cost of traversing the index
     */
    public static int searchCost(int numblocks, int rpb){
        // return numblocks / HashIndex.NUM_BUCKETS;
        return 1;
    }

}
