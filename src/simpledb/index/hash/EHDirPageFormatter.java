package simpledb.index.hash;

import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;

/**
 * Created by pablo on 4/21/17.
 */
public class EHDirPageFormatter implements PageFormatter {
    private TableInfo ti;

    @Override
    public void format(Page page) {
        page.setInt(0, 0); // Num records
        int recsize = ti.recordLength();
        for (int pos=INT_SIZE; pos + recsize <= BLOCK_SIZE; pos += recsize)
            makeDefaultRecord(page, pos);
    }

    EHDirPageFormatter(TableInfo ti) {
        this.ti = ti;
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
