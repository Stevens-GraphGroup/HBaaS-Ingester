package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.biojava3.core.sequence.ProteinSequence;

/**
 * Insert protein seqeunces into Accumulo following the schema in the project docs.
 */
public class ProteinSeqIngest {
    private static final Logger log = LogManager.getLogger(ProteinSeqIngest.class);


    D4MTableWriter d4MTables;
    TableWriter TseqRaw, TseqTDeg;

    //final String CF="", CQ="";
    final Text CF=TableWriter.EMPTYCF,
            CQseq=new Text("seq"),
            CQdesc=new Text("desc"),
            CQdeg=D4MTableWriter.DEFAULT_DEGCOL;

    public ProteinSeqIngest(Connector conn) {
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.useTable = config.useTableT  = true;
        config.useTableTDeg = config.useTableDeg = false;
        d4MTables = new D4MTableWriter("Tseq",conn,config);
        d4MTables.setCF(CF);
        TseqRaw = new TableWriter("TseqRaw",conn);
        TseqTDeg = new TableWriter("TseqTDeg",conn,D4MTableWriter.makeDegreeATC(CF,CQdeg));
    }

    /**
     * Create the tables to ingest to if they do not already exist.
     */
    public void createTablesSoft() {
        d4MTables.createTablesSoft();
        TseqRaw.createTablesSoft();
        TseqTDeg.createTablesSoft();
    }

    public void flushBuffers() {
        d4MTables.flushBuffers();
        TseqRaw.flushBuffer();
        TseqTDeg.flushBuffer();
    }

    public void closeIngest() {
        d4MTables.closeIngest();
        TseqRaw.closeIngest();
        TseqTDeg.closeIngest();
    }

    /**
     *
     * @param accID     "BAC05839.1"
     * @param seqRaw
     * @param desc      NULLABLE "seven transmembrane helix receptor, partial [Homo sapiens]"
     * @param seqProps  NULLABLE "gi|21928500", "dbj|BAC05839.1"
     */
    private void putSeqParts(String accID, String seqRaw, String desc, String... seqProps) {
        Text accIDt = new Text(accID);
        TseqRaw.ingestRow(accIDt, CQseq, new Value(seqRaw.getBytes()));
        if (desc != null && !desc.isEmpty())
            TseqRaw.ingestRow(accIDt,CQdesc, new Value(desc.getBytes()));

        if (seqProps != null)
            for (String seqProp : seqProps) {
                Text pt = new Text(seqProp);
                String ptStr = pt.toString();
                String field = ptStr.substring(0, ptStr.lastIndexOf('|'));
                Text fieldTxt = new Text(field);

                d4MTables.ingestRow(accIDt, pt);
                TseqTDeg.ingestRow(fieldTxt,CF,CQdeg);
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
