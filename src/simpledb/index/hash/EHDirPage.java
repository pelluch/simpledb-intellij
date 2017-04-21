package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.index.btree.BTreeDir;
import simpledb.index.btree.DirEntry;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;

/**
 * Created by pablo on 4/21/17.
 */
public class EHDirPage {

    private Block currentblk;
    private TableInfo ti;
    private Transaction tx;
    private int slotsize;
    private int currentslot = -1;

    public EHDirPage(Block currentblk, TableInfo ti, Transaction tx) {
        this.currentblk = currentblk;
        this.ti = ti;
        this.tx = tx;
        slotsize = ti.recordLength();
        tx.pin(currentblk);
    }

    public void close() {
        if (currentblk != null) {
            tx.unpin(currentblk);
            currentblk = null;
        }
    }

    public boolean isFull() {
        return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
    }

    public void insertDir(int bucket) {
        insert();
        setBucket(getNumRecs() - 1, bucket);
    }

    private int getBucket(int slot) {
        int pos = slotpos(slot);
        return tx.getInt(currentblk, pos);
    }

    private void setBucket(int slot, int val) {
        int pos = slotpos(slot);
        tx.setInt(currentblk, pos, val);
    }

    private void setNumRecs(int n) {
        tx.setInt(currentblk, 0, n);
    }
    private void insert() {
        setNumRecs(getNumRecs() + 1);
    }

    public int getNumRecs() {
        return tx.getInt(currentblk, 0);
    }

    private int slotpos(int slot) {
        return INT_SIZE + (slot * slotsize);
    }

    public void insert(int bucket) {
        if(isFull()) {
            throw new IllegalStateException("Bucket is full");
        }
        int numRecs = getNumRecs();
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null)
            return;
        BTreeDir root = new BTreeDir(rootblk, dirTi, tx);
        DirEntry e2 = root.insert(e);
        if (e2 != null)
            root.makeNewRoot(e2);
        root.close();
    }

}
