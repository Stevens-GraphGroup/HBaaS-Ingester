package edu.stevens;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

/**
 * Wrapper around the following tables:
 * table,
 * tableT,
 * tableDeg,
 * tableTDeg.
 * Can set the column family if you want to use a non-empty one.
 */
public class D4MTableWriter {
    private static final Logger log = LogManager.getLogger(TableWriter.class);

    public static final Text DEFAULT_DEGCOL = new Text("deg");
    public static class D4MTableConfig implements Cloneable {
        public boolean
                useTable = false,
                useTableT = false,
                useTableDeg = false,
                useTableTDeg = false;
        public Text textDegCol = DEFAULT_DEGCOL;
        /** The number of bytes until we flush data to the server. */
        public long batchBytes = 2_000_000L;

        public D4MTableConfig() {}

        public D4MTableConfig(D4MTableConfig c) {
            useTable = c.useTable;
            useTableT = c.useTableT;
            useTableDeg = c.useTableDeg;
            useTableTDeg = c.useTableTDeg;
            textDegCol = c.textDegCol;
            batchBytes = c.batchBytes;
        }

//        @Override
//        public D4MTableConfig clone() throws CloneNotSupportedException {
//            return (D4MTableConfig)super.clone();
//        }
    }
    private D4MTableConfig d4MTableConfig;

    private Text CF = TableWriter.EMPTYCF;
    public Text getCF() {return CF;}
    public void setCF(Text CF) {this.CF = CF;}

    private TableWriter
            table=null,
            tableT=null,
            tableDeg=null,
            tableTDeg=null;

    public static TableWriter.AfterTableCreate makeDegreeATC(final Text cf, final Text degCol) {
        return new TableWriter.AfterTableCreate() {
            @Override
            public void afterTableCreate(String tableName, Connector c) {
                IteratorSetting cfg = new IteratorSetting(19, "sum", SummingCombiner.class);
                Combiner.setColumns(cfg, Collections.singletonList(new IteratorSetting.Column(cf, degCol)));
                Combiner.setCombineAllColumns(cfg, false);
                LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
                try {
                    c.tableOperations().attachIterator(tableName, cfg);
                } catch (AccumuloSecurityException | AccumuloException e) {
                    log.warn("error trying to add iterator to "+tableName, e);
                } catch (TableNotFoundException e) {
                    log.error("impossible!",e);
                }
            }
        };
    }

    public D4MTableWriter(String baseName, Connector conn, D4MTableConfig config) {
        d4MTableConfig = new D4MTableConfig(config); // no aliasing
        if (config.useTable   ) table =    new TableWriter(baseName,conn,config.batchBytes);
        if (config.useTableT  ) tableT =   new TableWriter(baseName+"T",conn,config.batchBytes);
        TableWriter.AfterTableCreate atc = makeDegreeATC(CF, d4MTableConfig.textDegCol);
        if (config.useTableDeg) {
            tableDeg = new TableWriter(baseName+"Deg", conn, atc, config.batchBytes);
        }
        if (config.useTableTDeg) {
            tableTDeg = new TableWriter(baseName + "TDeg", conn, atc, config.batchBytes);
        }
    }



    /**
     * Create the tables to ingest to if they do not already exist.
     * Sets up iterators on degree tables if enabled.
     */
    public void createTablesSoft() {
        if (table != null) table.createTablesSoft();
        if (tableT != null) tableT.createTablesSoft();
        if (tableDeg != null) tableDeg.createTablesSoft();
        if (tableTDeg != null) tableTDeg.createTablesSoft();
    }

    public void flushBuffers() {
        if (table != null) table.flushBuffer();
        if (tableT != null) tableT.flushBuffer();
        if (tableDeg != null) tableDeg.flushBuffer();
        if (tableTDeg != null) tableTDeg.flushBuffer();
    }

    /**
     * Close all enabled table batch writers.
     */
    public void closeIngest() {
        if (table != null) table.closeIngest();
        if (tableT != null) tableT.closeIngest();
        if (tableDeg != null) tableDeg.closeIngest();
        if (tableTDeg != null) tableTDeg.closeIngest();
    }

    /** Use "1" as the Value. */
    public void ingestRow(Text rowID, Text cq) {
        ingestRow(rowID, cq, TableWriter.VALONE);
    }
    /** Ingest to all enabled tables. Use "1" for the degree table values. */
    public void ingestRow(Text rowID, Text cq, Value v) {
        if (table != null) table        .ingestRow(rowID, CF, cq, v);
        if (tableT != null) tableT      .ingestRow(cq, CF, rowID, v);
        if (tableDeg != null) tableDeg  .ingestRow(rowID, CF, d4MTableConfig.textDegCol, TableWriter.VALONE);
        if (tableTDeg != null) tableTDeg.ingestRow(cq, CF, d4MTableConfig.textDegCol, TableWriter.VALONE);
    }


}
