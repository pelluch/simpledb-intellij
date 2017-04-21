package simpledb.index.hash;

import simpledb.file.Block;
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

    public boolean next() {
        ++currentslot;
        if(currentslot >= getNumRecs()) {
            return false;
        }
        return true;
    }

    private boolean isFull() {
        return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
    }

    boolean insertDir(int bucket) {
        if(isFull()) {
            return false;
        }
        insert();
        setBucket(getNumRecs() - 1, bucket);
        return true;
    }

    int getCurrentBucket() {
        return tx.getInt(currentblk, slotpos(currentslot));
    }

    int getBucket(int slot) {
        int pos = slotpos(slot);
        return tx.getInt(currentblk, pos);
    }

    void setBucket(int slot, int val) {
        int pos = slotpos(slot);
        tx.setInt(currentblk, pos, val);
    }

    private void setNumRecs(int n) {
        tx.setInt(currentblk, 0, n);
    }
    private void insert() {
        if(isFull()) {
            throw new IllegalStateException("Bucket is full");
        }
        setNumRecs(getNumRecs() + 1);
    }

    int getNumRecs() {
        return tx.getInt(currentblk, 0);
    }

    private int slotpos(int slot) {
        return INT_SIZE + (slot * slotsize);
    }

    int getMaxCapacity() {
        return (BLOCK_SIZE - INT_SIZE) / slotsize;
    }

    static int getMaxCapacity(TableInfo info) {
        return (BLOCK_SIZE - INT_SIZE) / info.recordLength();
    }

    void printAll(int globalDepth, TableInfo bi) {
        System.out.println("Dir entries:");
        currentblk = new Block(ti.fileName(), 0);
        for(int i = 0; i < getNumRecs(); ++i) {
            String entry =
                    String.format("%" + globalDepth + "s",
                            Integer.toBinaryString(i)).replace(' ', '0');
            System.out.print(entry + " --> ");
            int numBucket = getBucket(i);
            System.out.println(numBucket);
            EHBucket bucket = new EHBucket(
                    new Block(bi.fileName(), numBucket),
                    bi,
                    null,
                    tx
            );
            bucket.print();
            bucket.close();
        }
        System.out.println("------");
    }

}
