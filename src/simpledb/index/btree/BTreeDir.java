package simpledb.index.btree;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.TableInfo;
import simpledb.query.Constant;

/**
 * A B-tree directory block.
 * @author Edward Sciore
 */
public class BTreeDir {
   private TableInfo ti;
   private Transaction tx;
   private String filename;
   private BTreePage contents;
   private int currentSlot = -1;

   void print() {
      contents.print();
   }

   void printTree(int depth) {
      StringBuilder tabs = new StringBuilder();
      for(int i = 0; i < depth; ++i) {
         tabs.append(depth);
      }
   }
   void printTree() {
      printTree(0);
   }
   /**
    * Creates an object to hold the contents of the specified
    * B-tree block.
    * @param blk a reference to the specified B-tree block
    * @param ti the metadata of the B-tree directory file
    * @param tx the calling transaction
    */
   BTreeDir(Block blk, TableInfo ti, Transaction tx) {
      this.ti = ti;
      this.tx = tx;
      filename = blk.fileName();
      contents = new BTreePage(blk, ti, tx);
   }

   void setKey(int slot, Constant key) {
      contents.setKey(slot, key);
   }
   /**
    * Closes the directory page.
    */
   public void close() {
      contents.close();
   }

   /**
    * Returns the block number of the B-tree leaf block
    * that contains the specified search key.
    * @param searchkey the search key value
    * @return the block number of the leaf block containing that search key
    */
   public BTSearchResult search(Constant searchkey) {
      Block childblk = findChildBlock(searchkey);
      int leaf;
      int parent = contents.getBlockNum();
      while (contents.getFlag() > 0) {
         parent = childblk.number();
         contents.close();
         contents = new BTreePage(childblk, ti, tx);
         childblk = findChildBlock(searchkey);
      }
      leaf = childblk.number();
      BTSearchResult result = new BTSearchResult();
      result.leaf = leaf;
      result.parent = parent;
      result.parentSlot = currentSlot;
      // contents.print();
      return result;
   }

   /**
    * Creates a new root block for the B-tree.
    * The new root will have two children:
    * the old root, and the specified block.
    * Since the root must always be in block 0 of the file,
    * the contents of the old root will get transferred to a new block.
    * @param e the directory entry to be added as a child of the new root
    */
   public void makeNewRoot(DirEntry e) {
      Constant firstval = contents.getDataVal(0);
      int level = contents.getFlag();
      Block newblk = contents.split(0, level); //ie, transfer all the records
      DirEntry oldroot = new DirEntry(firstval, newblk.number());
      insertEntry(oldroot);
      insertEntry(e);
      contents.setFlag(level+1);
   }

   /**
    * Inserts a new directory entry into the B-tree block.
    * If the block is at level 0, then the entry is inserted there.
    * Otherwise, the entry is inserted into the appropriate
    * child node, and the return value is examined.
    * A non-null return value indicates that the child node
    * split, and so the returned entry is inserted into
    * this block.
    * If this block splits, then the method similarly returns
    * the entry information of the new block to its caller;
    * otherwise, the method returns null.
    * @param e the directory entry to be inserted
    * @return the directory entry of the newly-split block, if one exists; otherwise, null
    */
   public DirEntry insert(DirEntry e) {
      if (contents.getFlag() == 0)
         return insertEntry(e);
      Block childblk = findChildBlock(e.dataVal());
      BTreeDir child = new BTreeDir(childblk, ti, tx);
      DirEntry myentry = child.insert(e);
      child.close();
      return (myentry != null) ? insertEntry(myentry) : null;
   }

   private DirEntry insertEntry(DirEntry e) {
      int newslot = 1 + contents.findSlotBefore(e.dataVal());
      contents.insertDir(newslot, e.dataVal(), e.blockNumber());
      if (!contents.isFull())
         return null;


      // else page is full, so split it
      int level = contents.getFlag();
      int splitpos = contents.getNumRecs() / 2;
      Constant splitval = contents.getDataVal(splitpos);
      Block newblk = contents.split(splitpos, level);
      return new DirEntry(splitval, newblk.number());
   }

   private Block findChildBlock(Constant searchkey) {
      currentSlot = contents.findSlotBefore(searchkey);
      if (contents.getDataVal(currentSlot+1).equals(searchkey))
         currentSlot++;
      int blknum = contents.getChildNum(currentSlot);
      return new Block(filename, blknum);
   }
}
