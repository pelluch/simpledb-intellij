package simpledb.index.hash;

import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * Created by pablo on 4/21/17.
 */
public class EHDir {
    private TableInfo ti;
    private Transaction tx;
    private EHDirPage contents;
    private int globalDepth;
    private int dirSize;

    EHDir(String idxname, Transaction tx) {
        this.tx = tx;
        Schema dirSchema = new Schema();
        dirSchema.addIntField("bucket");
        String dirTable = idxname + "dir";
        ti = new TableInfo(dirTable, dirSchema);

        int numBlocks = tx.size(ti.fileName());
        Block lastDirBlock;
        if(numBlocks == 0) {
            lastDirBlock = tx.append(ti.fileName(), new EHDirPageFormatter(ti));
            ++numBlocks;
        } else {
            lastDirBlock = new Block(ti.fileName(), numBlocks - 1);
        }

        contents = new EHDirPage(lastDirBlock, ti, tx);
        int numRecords = contents.getNumRecs();

        if(numRecords == 0) {
            contents.insertDir(0);
            dirSize = 1;
        } else {
            dirSize = contents.getMaxCapacity() * (numBlocks - 1)
                    + numRecords;
            assert dirSize == 1 || dirSize % 2 == 0;
            globalDepth = Integer.numberOfTrailingZeros(dirSize);
        }
    }

    int getGlobalDepth() {
        return globalDepth;
    }

    void increaseGlobalDepth() {
        int bucketsRead = 0;
        int[] bucketPointers = new int[dirSize];
        contents.close();
        contents = new EHDirPage(
                new Block(ti.fileName(), 0),
                ti,
                tx
        );
        int currentBlock = 0;
        while(bucketsRead < dirSize) {
            if(!contents.next()) {
                contents.close();
                ++currentBlock;
                contents = new EHDirPage(
                        new Block(ti.fileName(), currentBlock),
                        ti,
                        tx
                );
                contents.next();
            }
            bucketPointers[bucketsRead++] =
                    contents.getCurrentBucket();
        }

        for (int bucketPointer : bucketPointers) {
            if (!contents.insertDir(bucketPointer)) {
                contents.close();
                Block newBlock = tx.append(ti.fileName(),
                        new EHDirPageFormatter(ti));
                contents = new EHDirPage(
                        newBlock,
                        ti,
                        tx
                );
            }
        }

        contents.close();
        dirSize <<= 1;
        ++globalDepth;
        System.out.println("OK now what");
    }

    public void close() {
        contents.close();
    }

    void updateEntries(Constant searchkey, int localDepth, int newBucket) {
        if(localDepth < 0) {
            throw new IllegalArgumentException("Local depth must be >= 0");
        }
        // We must look for entries starting with this
        int prefix = ~(-1 << localDepth) & searchkey.hashCode() | (1 << localDepth);
        int dirMask = ~(-1 << localDepth + 1);


        int currentBlock = -1;
        for(int i = 0; i < dirSize; ++i) {
            int masked = dirMask & i;
            if(masked == prefix) {
                int block = i / contents.getMaxCapacity();
                int offset = i % contents.getMaxCapacity();
                if(currentBlock != block) {
                   contents.close();
                   contents = new EHDirPage(new Block(ti.fileName(),
                           block),
                           ti,
                           tx);
                    currentBlock = i;
                }
                contents.setBucket(offset, newBucket);
                contents.printAll();
            }
        }
    }

    int getBucketForKey(Constant searchkey) {
        int mask = globalDepth == 0 ? 0 : ~(-1 << globalDepth);
        int hash = searchkey.hashCode();
        int dirIdx = hash & mask;
        int numDirBlock = dirIdx / EHDirPage.getMaxCapacity(ti);
        int bucketOffset = dirIdx % EHDirPage.getMaxCapacity(ti);
        Block dirBlock = new Block(ti.fileName(), numDirBlock);
        EHDirPage page = new EHDirPage(dirBlock, ti, tx);
        int numBucket = page.getBucket(bucketOffset);
        page.close();
        return numBucket;
    }
}
