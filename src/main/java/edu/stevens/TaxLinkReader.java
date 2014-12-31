package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.accumulo.core.security.Authorizations;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Assumes that gi_taxid_prot.dmp is in increasing gi order.
 */
public class TaxLinkReader {
    private static final Logger log = LogManager.getLogger(TaxReader.class);
    private final Connector connector;
    private final D4MTableWriter d4mtw;
    private final TableWriter twTDeg;
    public static final Text DEG_CHILD_ACC = new Text("degChildAcc");
    private boolean isClosed = false;

    private int countOk = 0;
    private BufferedWriter bw = null;

    public TaxLinkReader(Connector connector) {
        this.connector = connector;
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.baseName = "Ttax";
        config.useTable = config.useTableT = true;
        config.useTableDeg = config.useTableTDeg = false;
        config.connector = connector;
        d4mtw = new D4MTableWriter(config);
        d4mtw.createTablesSoft();
        twTDeg = new TableWriter("TtaxTDeg", connector, D4MTableWriter.makeDegreeATC()); //TableWriter.EMPTYCF, DEG_CHILD_ACC
        twTDeg.createTablesSoft();
        try {
            bw = new BufferedWriter(new FileWriter("giNotInDB.dmp"));
        } catch (IOException e) {
            log.warn("unable to create writer of bad sequences to file: giNotInDB.dmp.  Will not log gi's not in Accumulo DB.", e);
            bw = null;
        }
    }

    public void close() {
        isClosed = true;
        d4mtw.closeIngest();
        twTDeg.closeIngest();
        if (bw != null)
            try {
                bw.close();
            } catch (IOException ein) {
                log.warn("failed to close BatchWriter to giNotInDB.dmp during close() method; assigning to null",ein);
            }
        bw = null;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    public void ingestTaxLinkFile(File file) throws IOException {
        if (isClosed)
            throw new IllegalStateException(this+" isClosed");
        Scanner scanner;
        try {
            scanner = connector.createScanner("TseqT", Authorizations.EMPTY);
        } catch (TableNotFoundException e) {
            log.error("table TseqT does not exist",e);
            return;
        }

        Range range = new Range(new Text("gi|"), new Text("gj"));
        scanner.setRange(range);
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext()) {
            log.warn("scanner returned nothing; no entries ingested!");
            return;
        }
        Key k = iterator.next().getKey();

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            k = ingestTaxLinkLine(k, iterator, line);
            count++;
        }
        reader.close();
        scanner.close();
        d4mtw.flushBuffers();
        bw.flush();
        log.info("TaxLinkReader: ingestTaxLinkFile: out of "+count+" gi-taxID pairs from file "+file.getName()+". ingested "+countOk+" accID-taxID pairs (that have gi's in the database)");
    }

    private void writeBadGTToFile(int giBad) {
        if (bw == null) {
            return;
        }
        try {
            bw.write(Integer.toString(giBad));
            bw.write('\n');
        } catch (IOException e) {
            log.warn("error writing to giNotInDB.dmp",e);
            try {
                bw.close();
            } catch (IOException ein) {
                log.warn("failed to close BatchWriter to giNotInDB.dmp after error; assigning to null",ein);
            }
            bw = null;
        }
    }


    private Key ingestTaxLinkLine(Key k, Iterator<Map.Entry<Key, Value>> iterator, String line) {
        String gi, taxID;
        {
            int pos = line.indexOf('\t');
            if (pos == -1)
                return k;
            gi = line.substring(0,pos);
            taxID = line.substring(pos+1);
        }
        int giTarget = Integer.parseInt(gi);


        String giString = k.getRow().toString().substring(3); // gi|
        int giDB = Integer.parseInt( giString );

        //log.debug("giTarget="+giTarget+"\tgiDB="+giDB);
        while (giDB < giTarget && iterator.hasNext()) {
            //log.debug("giTarget="+giTarget+"\tgiDB="+giDB);
            k = iterator.next().getKey();
            giString = k.getRow().toString().substring(3); // gi|
            giDB = Integer.parseInt( giString );
        }
        if (giDB != giTarget) {
            if (bw == null)
                log.debug("no bad gi file writer and cannot find gi " + giTarget + " in database (next gi is " + giDB + " or no more gi's in database)");
            else
                writeBadGTToFile(giTarget);
        } else {
            Text accID = k.getColumnQualifier();
            Text formattedTaxID = TaxReader.formatTaxID(taxID);
            d4mtw.ingestRow(accID, formattedTaxID);
            twTDeg.ingestRow(formattedTaxID, DEG_CHILD_ACC);
            countOk++;
        }
        return k;
    }


}
