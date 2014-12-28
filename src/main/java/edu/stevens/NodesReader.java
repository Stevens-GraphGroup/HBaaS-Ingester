package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodesReader {
    private static final Logger log = LogManager.getLogger(NodesReader.class);
    private final Map<Integer, String> divInfo;
    private final Connector connector;
    private final D4MTableWriter d4mtw;

    public NodesReader(Map<Integer,String> divInfo, Connector connector) {
        this.divInfo = divInfo;
        this.connector = connector;
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.baseName = "Ttax";
        config.useTable = config.useTableT = true;
        config.useTableDeg = config.useTableTDeg = false;
        config.connector = connector;
        d4mtw = new D4MTableWriter(config);
    }

    public void ingestFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            ingestLine(line);
            count++;
        }
        d4mtw.flushBuffers();
        log.info("NodesReader: ingested "+count+" taxIDs in file "+file.getName());
    }

    public void ingestLine(String line) {
        List<String> fields = DivisionReader.readLine(line);
        Text taxID =         processTaxID(fields.get(0));     // 0002077
        Text parentTaxID =   processTaxID(fields.get(1));     // 0186821
        Text rank =          new Text("rank|"+fields.get(2) );         // genus
        Text divisionDesc =  new Text("division|"+divInfo.get(Integer.parseInt(fields.get(4))));

        if (!taxID.toString().equals("1"))
            d4mtw.ingestRow(taxID, parentTaxID);
        d4mtw.ingestRow(taxID, rank);
        d4mtw.ingestRow(taxID, divisionDesc);
    }

    public final int PADLENGTH_taxid = 7;

    private Text processTaxID(String taxID) {
        String padded = StringUtils.leftPad(taxID, PADLENGTH_taxid, '0');  // pad to 9 digits
        return new Text("taxid|"+padded);
    }


}
