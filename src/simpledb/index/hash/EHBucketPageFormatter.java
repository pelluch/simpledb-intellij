package simpledb.index.hash;

import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;

public class EHBucketPageFormatter implements PageFormatter {

    private TableInfo ti;
    private int localDepth;

    @Override
    public void format(Page page) {
        page.setInt(0, localDepth);
        page.setInt(INT_SIZE, 0);  // #records = 0
        int recsize = ti.recordLength();
        for (int pos=2*INT_SIZE; pos+recsize<=BLOCK_SIZE; pos += recsize)
            makeDefaultRecord(page, pos);
    }

    EHBucketPageFormatter(TableInfo ti, int localDepth) {
        this.ti = ti;
        this.localDepth = localDepth;
    }

    private void makeDefaultRecord(Page page, int pos) {
        for (String fldname : ti.schema().fields()) {
            int offset = ti.offset(fldname);
            if (ti.schema().type(fldname) == INTEGER)
                page.setInt(pos + offset, 0);
            else
                page.setString(pos + offset, "");
        }
    }

}
