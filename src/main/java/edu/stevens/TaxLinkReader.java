package edu.stevens;

import org.apache.accumulo.core.client.Connector;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Assumes that gi_taxid_prot.dmp is in increasing gi order.
 */
public class TaxLinkReader {
    private static final Logger log = LogManager.getLogger(TaxReader.class);
    private final Connector connector;
    private final D4MTableWriter d4mtw;
    private boolean isClosed = false;

    private List<Integer> giNotInDB = new LinkedList<>();
    private int countOk = 0;

    public TaxLinkReader(Connector connector) {
        this.connector = connector;
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.baseName = "Ttax";
        config.useTable = config.useTableT = true;
        config.useTableDeg = config.useTableTDeg = false;
        config.connector = connector;
        d4mtw = new D4MTableWriter(config);
        d4mtw.createTablesSoft();
    }

    public void close() {
        isClosed = true;
        d4mtw.closeIngest();
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
        writeBadGTToFile();
        log.info("TaxLinkReader: ingestTaxLinkFile: out of "+count+" gi-taxID pairs from file "+file.getName()+". ingested "+countOk+" accID-taxID pairs (that have gi's in the database)");
    }

    private void writeBadGTToFile() {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("giNotInDB.dmp"));
            for (Integer giBad : giNotInDB) {
                bw.write(giBad.toString());
                bw.write('\n');
            }
            bw.close();
        } catch (IOException e) {
            log.warn("error writing to giNotInDB.dmp",e);
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
            log.debug("cannot find gi " + giTarget + " in database (next gi is " + giDB + " or no more gi's in database)");
            giNotInDB.add(giTarget);
        } else {
            Text accID = k.getColumnQualifier();
            d4mtw.ingestRow(accID, TaxReader.formatTaxID(taxID));
            countOk++;
        }
        return k;
    }


}
