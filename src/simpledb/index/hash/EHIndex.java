package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.*;
import simpledb.tx.Transaction;

/**
 * Created by pablo on 4/20/17.
 */
public class EHIndex implements Index {

    private int globalDepth = 0;
    private int localDepth = 0;
    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;
    private TableInfo dirInfo;
    private TableInfo bucketInfo;

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
            tx.append(bucketInfo.fileName(), new EHBucketPageFormatter(bucketInfo, 0));
        }

        Schema dirSchema = new Schema();
        dirSchema.addIntField("bucket");
        String dirTable = idxname + "dir";
        dirInfo = new TableInfo(dirTable, dirSchema);

        Block lastDirBlock;
        int numBlocks = tx.size(dirInfo.fileName());

        if(numBlocks == 0) {
            lastDirBlock = tx.append(dirInfo.fileName(), new EHDirPageFormatter(dirInfo));
            ++numBlocks;
        } else {
            lastDirBlock = new Block(dirInfo.fileName(), numBlocks - 1);
        }
        // RecordPage page = new RecordPage(lastDirBlock, dirInfo, tx);
        EHDirPage page = new EHDirPage(lastDirBlock, dirInfo, tx);
        int numRecords = page.getNumRecs();
        if(numRecords == 0) {
            page.insert();
            page.setInt("bucket", 0);
        } else {
            int dirSize =
        }

        page.close();

    }

    @Override
    public void beforeFirst(Constant searchkey) {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public RID getDataRid() {
        return null;
    }

    @Override
    public void insert(Constant dataval, RID datarid) {

    }

    @Override
    public void delete(Constant dataval, RID datarid) {

    }

    @Override
    public void close() {

    }
}
