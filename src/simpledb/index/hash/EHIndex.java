package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
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
