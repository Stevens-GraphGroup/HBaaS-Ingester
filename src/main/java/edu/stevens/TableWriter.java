package edu.stevens;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

/**
 * Wraps an Accumulo table for writing.
 * Flushes write cache after number of bytes to write exceeds batchBytes.
 */
public class TableWriter {
    private static final Logger log = LogManager.getLogger(TableWriter.class);
    private String name;
    //private long cacheWriteBytes;
    private final Connector connector;
    private BatchWriter BW=null;
    private AfterTableCreate atc=null;

    public static enum State { New, Open, Closed };
    private State state = State.New;
    State getState() { return state; }

    /** A method to do something right after creating the table. */
    public static interface AfterTableCreate {
        public void afterTableCreate(String tableName, Connector c);
    }

    /** A method to create a batch writer. Two use cases: create a plain old bw,
     * and create one from a MultiTableBatchWriter. */
    public static interface BatchWriterCreate {
        public BatchWriter createBatchWriter(String tableName, Connector c) throws TableNotFoundException;
    }
    private BatchWriterCreate batchWriterCreate;
    public static BatchWriterCreate createRegularBatchWriter(final long batchBytes) {
        return new BatchWriterCreate() {
            @Override
            public BatchWriter createBatchWriter(String tableName, Connector c) throws TableNotFoundException {
                BatchWriterConfig BWconfig = new BatchWriterConfig();
                BWconfig.setMaxMemory(batchBytes); // bytes available to batchwriter for buffering mutations
                return c.createBatchWriter(tableName, BWconfig);
            }
        };
    }

    /** The number of bytes until we flush data to the server. */
    public final static long DEFAULT_BATCHBYTES = 2_000_000L;

    private long totalBytesToWrite = 0l;
    public long getTotalBytesToWrite() { return totalBytesToWrite; }

    public TableWriter(String name, Connector conn) {
        this.name = name;
        this.connector = conn;
        batchWriterCreate = createRegularBatchWriter(DEFAULT_BATCHBYTES);
    }

    public TableWriter(String name, Connector conn, BatchWriterCreate bwc) {
        this(name,conn);
        batchWriterCreate = bwc;
    }

    public TableWriter(String name, Connector conn, AfterTableCreate atc) {
        this(name,conn);
        this.atc = atc;
        batchWriterCreate = createRegularBatchWriter(DEFAULT_BATCHBYTES);
    }

    public TableWriter(String name, Connector conn, AfterTableCreate atc, BatchWriterCreate bwc) {
        this(name,conn,atc);
        batchWriterCreate = bwc;
    }

    /**
     * Create the tables to ingest to if they do not already exist.
     * Call the atc if provided.
     */
    public void createTablesSoft() {
        TableOperations to = connector.tableOperations();
        try {
            if (!to.exists(name))
                to.create(name);
            // Add iterators and such
            if (atc != null)
                atc.afterTableCreate(name, connector);
        } catch (AccumuloException | AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (TableExistsException e) {
            log.error("impossible! Table checked to be created!", e);
        }
    }

    public void openIngest() {
        switch(state) {
            case New: createTablesSoft(); break;
            case Open: throw new IllegalStateException("tried to open ingset when already open");
            case Closed: break;
        }

        try {
            BW = batchWriterCreate.createBatchWriter(name, connector);
        } catch (TableNotFoundException e) {
            log.error("impossible! Tables should have been created!", e);
        }
        state = State.Open;
    }

    public void closeIngest() {
        if (state != State.Open)
            throw new IllegalStateException("tried to close when already closed");
        try {
            BW.close();
        } catch (MutationsRejectedException e) {
            log.warn("trouble closing BatchWriter",e);
        }
        BW = null;
        state = State.Closed;
    }

    public void flushBuffer() {
        if (state != State.Open)
            throw new IllegalStateException("flushing buffer when not open");
        try {
            BW.flush();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    public static final Value VALONE = new Value("1".getBytes());
    public static final Text EMPTYCF =new Text("");

    /** Use "" as column family, "1" as Value. */
    public void ingestRow(Text rowID, Text cq) {
        ingestRow(rowID, EMPTYCF, cq, VALONE);
    }
    /** Use "" as column family. */
    public void ingestRow(Text rowID, Text cq, Value v) {
        ingestRow(rowID, EMPTYCF, cq, v);
    }
    /** Use "1" as the Value. */
    public void ingestRow(Text rowID, Text cf, Text cq) {
        ingestRow(rowID, cf, cq, VALONE);
    }
    public void ingestRow(Text rowID, Text cf, Text cq, Value v) {
        if (state != State.Open)
            openIngest();
        Mutation m = new Mutation(rowID);
        m.put(cf, cq, v);
        try {
            BW.addMutation(m);
        } catch (MutationsRejectedException e) {
            log.warn("mutation rejected: (row,cf,cq,v)=("+rowID+','+cf+','+cq+','+v+")",e);
        }
    }
}
