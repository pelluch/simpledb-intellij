package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.Console;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;

/**
 * Created by pablo on 4/21/17.
 */
public class EHBucketPage {
    private Block currentblk;
    private TableInfo ti;
    private Transaction tx;
    private int slotsize;

    EHBucketPage(Block currentblk, TableInfo ti, Transaction tx) {
        this.currentblk = currentblk;
        this.ti = ti;
        this.tx = tx;
        slotsize = ti.recordLength();
        tx.pin(currentblk);
    }

    int findSlotBefore(Constant searchkey) {
        int slot = 0;
        while (slot < getNumRecs() && !getDataVal(slot).equals(searchkey))
            slot++;
        return slot - 1;
    }

    int getNumRecs() {
        return tx.getInt(currentblk, INT_SIZE);
    }


    int getLocalDepth() {
        return tx.getInt(currentblk, 0);
    }

    int increaseLocalDepth() {
        int localDepth = getLocalDepth() + 1;
        tx.setInt(currentblk, 0, localDepth);
        return localDepth;
    }

    /**
     * Returns the dataval of the record at the specified slot.
     * @param slot the integer slot of an index record
     * @return the dataval of the record at that slot
     */
    public Constant getDataVal(int slot) {
        return getVal(slot, "dataval");
    }

    private Constant getVal(int slot, String fldname) {
        int type = ti.schema().type(fldname);
        if (type == INTEGER)
            return new IntConstant(getInt(slot, fldname));
        else
            return new StringConstant(getString(slot, fldname));
    }

    int getBucketNumber() {
        return currentblk.number();
    }
    /**
     * Closes the page by unpinning its buffer.
     */
    public void close() {
        if (currentblk != null)
            tx.unpin(currentblk);
        currentblk = null;
    }

    private void setInt(int slot, String fldname, int val) {
        int pos = fldpos(slot, fldname);
        tx.setInt(currentblk, pos, val);
    }

    private void setString(int slot, String fldname, String val) {
        int pos = fldpos(slot, fldname);
        tx.setString(currentblk, pos, val);
    }
    private int getInt(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getInt(currentblk, pos);
    }

    private String getString(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getString(currentblk, pos);
    }

    private int fldpos(int slot, String fldname) {
        int offset = ti.offset(fldname);
        return slotpos(slot) + offset;
    }

    private void setVal(int slot, String fldname, Constant val) {
        int type = ti.schema().type(fldname);
        if (type == INTEGER)
            setInt(slot, fldname, (Integer)val.asJavaVal());
        else
            setString(slot, fldname, (String)val.asJavaVal());
    }


    private int slotpos(int slot) {
        return INT_SIZE + INT_SIZE + (slot * slotsize);
    }

    public void insertBucket(int slot, Constant val, RID rid) {
        setNumRecs(getNumRecs() + 1);
        setVal(slot, "dataval", val);
        setInt(slot, "block", rid.blockNumber());
        setInt(slot, "id", rid.id());
    }

    void setNumRecs(int n) {
        tx.setInt(currentblk, INT_SIZE, n);
    }

    public void printAll() {
        int numRecs = getNumRecs();
        System.out.println("\tNum Records: " + numRecs);
        for(int i = 0; i < numRecs; ++i) {
            Constant val = getVal(i, "dataval");
            int depth = getLocalDepth();
            int hash = val.hashCode();
            int mask = depth == 0 ? 0 : ~(-1 << depth);
            int masked = hash & mask;
            String entry =
                    String.format("%" + depth + "s",
                            Integer.toBinaryString(masked)).replace(' ', '0');
            System.out.println("\t" + entry + " - " + Integer.toBinaryString(hash) + " = " + val);
        }
    }
    public boolean isFull() {
        int newPos = slotpos(getNumRecs() + 1);
        return newPos >= BLOCK_SIZE;
    }

    public RID getDataRid(int slot) {
        return new RID(getInt(slot, "block"), getInt(slot, "id"));
    }
}
