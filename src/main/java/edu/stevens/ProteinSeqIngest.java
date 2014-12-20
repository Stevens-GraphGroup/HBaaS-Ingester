package edu.stevens;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.biojava3.core.sequence.ProteinSequence;

import java.util.Collections;

/**
 * Insert protein seqeunces into Accumulo following the schema in the project docs.
 */
public class ProteinSeqIngest {
    private static final Logger log = LogManager.getLogger(ProteinSeqIngest.class);
    final String    TNseq = "Tseq",
                    TNseqT = "TseqT",
                    TNseqRaw = "TseqRaw",
                    TNseqTDeg = "TseqTDeg";
    private final Connector connector;
    enum State { New, Open, Closed };
    private State state = State.New;
    State getState() { return state; }
    private BatchWriter Bseq=null, BseqT=null, BseqRaw=null, BseqTDeg=null;

    /** The number of bytes until we flush data to the server. */
    private long batchBytes = 2_000_000L;
    public long getBatchBytes() {
        return batchBytes;
    }
    public void setBatchBytes(int batchBytes) {
        this.batchBytes = batchBytes;
    }
    private long TseqBytes, TseqTBytes, TseqRawBytes, TseqTDegBytes;

    //final String CF="", CQ="";
    final Text CF =new Text(""),
            CQseq=new Text("seq"),
            CQdesc=new Text("desc"),
            CQdeg=new Text("deg");
    private static final Value VALONE = new Value("1".getBytes());



    // later make the table names configurable
//    public ProteinSeqIngest(String TNseq, String TNseqT, String TNseqRaw) {
//    }

    public ProteinSeqIngest(Connector conn) {
        this.connector = conn;
    }

    /**
     * Create the tables to ingest to if they do not already exist.
     */
    public void createTablesSoft() {
        TableOperations to = connector.tableOperations();
        String[] tns = new String[] {TNseq, TNseqT, TNseqRaw, TNseqTDeg };
        try {
            for (String tn : tns)
                if (!to.exists(tn))
                    to.create(tn);
            // add degree accumulator on TNseqTDeg
            IteratorSetting cfg = new IteratorSetting(5, "sum", SummingCombiner.class);
            Combiner.setColumns(cfg, Collections.singletonList(new IteratorSetting.Column(CF, CQdeg)));
            Combiner.setCombineAllColumns(cfg, false);
            LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
            // Add Iterator to table
            connector.tableOperations().attachIterator(TNseqTDeg, cfg);
        } catch (AccumuloException | AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (TableExistsException | TableNotFoundException e) {
            assert false : "impossible!";
        }
    }

    public void openIngest() {
        switch(state) {
            case New: createTablesSoft(); break;
            case Open: throw new IllegalStateException("tried to open ingset when already open");
            case Closed: break;
        }

        BatchWriterConfig BWconfig = new BatchWriterConfig();
        BWconfig.setMaxMemory(10_000_000L); // bytes available to batchwriter for buffering mutations
        try {
            Bseq = connector.createBatchWriter(TNseq, BWconfig);
            BseqT = connector.createBatchWriter(TNseqT, BWconfig);
            BseqRaw = connector.createBatchWriter(TNseqRaw, BWconfig);
            BseqTDeg = connector.createBatchWriter(TNseqTDeg, BWconfig);
        } catch (TableNotFoundException e) {
            e.printStackTrace();
            log.error("tables should have been created!", e);
            assert false;
        }
        TseqBytes = TseqTBytes = TseqRawBytes = TseqTDegBytes = 0l;
        state = State.Open;
    }

    public void closeIngest() {
        if (state != State.Open)
            throw new IllegalStateException("tried to close when already closed");
        try {
            Bseq.close();
            BseqT.close();
            BseqRaw.close();
            BseqTDeg.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
        Bseq = BseqT = BseqRaw = BseqTDeg = null;
        TseqBytes = TseqTBytes = TseqRawBytes = TseqTDegBytes = 0l;
        state = State.Closed;
    }

    /** Flush buffered data to Accumulo if it exceeds the batch amount. */
    private void checkFlushBuffers() throws MutationsRejectedException {
        if (state != State.Open)
            throw new IllegalStateException("checking to flush buffer when not open");
        if (TseqBytes >= batchBytes) {
            Bseq.flush();
            TseqBytes = 0l;
        }
        if (TseqTBytes >= batchBytes) {
            BseqT.flush();
            TseqTBytes = 0l;
        }
        if (TseqRawBytes >= batchBytes) {
            BseqRaw.flush();
            TseqRawBytes = 0l;
        }
        if (TseqTDegBytes >= batchBytes) {
            BseqTDeg.flush();
            TseqTDegBytes = 0l;
        }
    }

    public void flushBuffers() {
        if (state != State.Open)
            throw new IllegalStateException("flushing buffer when not open");
        try {
            Bseq.flush();
            BseqT.flush();
            BseqRaw.flush();
            BseqTDeg.flush();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
        TseqBytes = TseqTBytes = TseqRawBytes = 0l;
    }

    /**
     *
     * @param accID     "BAC05839.1"
     * @param seqRaw
     * @param desc      NULLABLE "seven transmembrane helix receptor, partial [Homo sapiens]"
     * @param seqProps  NULLABLE "gi|21928500", "dbj|BAC05839.1"
     */
    private void putSeqParts(String accID, String seqRaw, String desc, String... seqProps) {
        if (state == State.Closed)
            openIngest();
        try {
            Text accIDt = new Text(accID);
            Mutation m1 = new Mutation(accIDt);
            m1.put(CF, CQseq, new Value(seqRaw.getBytes()));
            if (desc != null && !desc.isEmpty())
                m1.put(CF, CQdesc, new Value(desc.getBytes()));
            m1.estimatedMemoryUsed();
            BseqRaw.addMutation(m1);
            TseqRawBytes += m1.numBytes();

            if (seqProps != null)
                for (String seqProp : seqProps) {
                    Text pt = new Text(seqProp);
                    String ptStr = pt.toString();

                    Mutation m2 = new Mutation(accIDt);
                    m2.put(CF,pt, VALONE);
                    Bseq.addMutation(m2);
                    TseqBytes += m2.numBytes();

                    Mutation m3 = new Mutation(pt);
                    m3.put(CF,accIDt,VALONE);
                    BseqT.addMutation(m3);
                    TseqTBytes += m3.numBytes();

                    String field = ptStr.substring(0, ptStr.lastIndexOf('|'));
                    Text fieldTxt = new Text(field);
                    Mutation m4 = new Mutation(fieldTxt);
                    m4.put(CF,CQdeg,VALONE);
                    BseqTDeg.addMutation(m4);
                    TseqTDegBytes += m4.numBytes();
                }
            checkFlushBuffers();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    public void putSeq(ProteinSequence ps) {
        putSeq(ps.getAccession().getID(),ps);
    }

    /** Zero-padding the gi field */
    public static final int PADLENGTH_gi = 9;

    public void putSeq(String accID, ProteinSequence ps) {
        String accIDfull = "accid|"+accID;
        String header = ps.getOriginalHeader(); // "gi|561733|gb|AAB63305.1| MHC class II DRA [Macaca mulatta]"

        int splitSpace = header.indexOf(' ');
        String seqID = splitSpace == -1 ? header : header.substring(0, splitSpace);
        String[] parts = seqID.split("\\|");
        if (parts.length >= 4 && parts[0].equals("gi") && parts[3].equals(accID)) {
            if (parts[1].length() > 9)  log.warn("warning: the sequence " + accID + " has a gi of " + parts[1] + " which is greater than " + PADLENGTH_gi + " characters. Reset the PADLENGTH.");
            String giID = "gi|" + StringUtils.leftPad(parts[1],PADLENGTH_gi,'0');  // pad to 9 digits
            String dbID = "db|" + parts[2] + '|' + accID;
            String desc = header.substring(splitSpace + 1).trim(); // remove surrounding whitespace
            putSeqParts(accIDfull, ps.getSequenceAsString(), desc, giID, dbID);
        } else {
            log.warn("bad original header on protein sequence: "+header);
            putSeqParts(accIDfull, ps.getSequenceAsString(), header);
        }
    }



}
