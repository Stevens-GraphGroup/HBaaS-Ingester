package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaxReader {
    private static final Logger log = LogManager.getLogger(TaxReader.class);
    private final Map<Integer, String> divInfo;
    private final Connector connector;
    private final D4MTableWriter d4mtw;

    public TaxReader(Map<Integer, String> divInfo, Connector connector) {
        this.divInfo = divInfo;
        this.connector = connector;
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.baseName = "Ttax";
        config.useTable = config.useTableT = true;
        config.useTableDeg = config.useTableTDeg = false;
        config.connector = connector;
        d4mtw = new D4MTableWriter(config);
    }

    public static Map<Integer, String> readDivisions(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Map<Integer,String> map = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            List<String> li = readTabPipeLine(line);
            Integer integer = Integer.parseInt(li.get(0));
            String desc = li.get(2);
            map.put(integer, desc);
        }
        return map;
    }

    /** "0\t|\tBCT\t|\tBacteria\t|\t\t|"; */
    //8	|	UNA	|	Unassigned	|	No species nodes should inherit this division assignment	|
    static List<String> readTabPipeLine(String line) {
        List<String> li = new ArrayList<>();

        while (true) {
            //System.out.println("LINE: "+line);
            int pos = line.indexOf('\t');
            assert (line.charAt(pos + 1) == '|');
            li.add(line.substring(0, pos));
            if (line.length() >= pos+3 && line.charAt(pos+2) == '\t')
                line = line.substring(pos + 3); // skip \t|
            else
                break;
        }
        return li;
    }

    public void ingestNodesFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            ingestNodesLine(line);
            count++;
        }
        d4mtw.flushBuffers();
        log.info("TaxReader: ingestNodesFile: ingested "+count+" taxIDs in file "+file.getName());
    }

    public void ingestNamesFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            ingestNamesLine(line);
            count++;
        }
        d4mtw.flushBuffers();
        log.info("TaxReader: ingestNamesFile: ingested "+count+" names in file "+file.getName());
    }

    public void ingestNodesLine(String line) {
        List<String> fields = readTabPipeLine(line);
        Text taxID =         formatTaxID(fields.get(0));     // 0002077
        Text parentTaxID =   formatTaxID(fields.get(1));     // 0186821
        Text rank =          new Text("rank|"+fields.get(2) );         // genus
        Text divisionDesc =  new Text("division|"+divInfo.get(Integer.parseInt(fields.get(4))));

        if (!taxID.toString().equals("1"))
            d4mtw.ingestRow(taxID, parentTaxID);
        d4mtw.ingestRow(taxID, rank);
        d4mtw.ingestRow(taxID, divisionDesc);
    }

    public static final int PADLENGTH_taxid = 7;

    static Text formatTaxID(String taxID) {
        String padded = StringUtils.leftPad(taxID, PADLENGTH_taxid, '0');  // pad to 9 digits
        return new Text("taxid|"+padded);
    }


    static final String SCINAME = "scientific name";

    // 7	|	Azorhizobium caulinodans	|		|	scientific name	|
    // 7	|	Azorhizobium caulinodans Dreyfus et al. 1988	|		|	synonym	|
    public void ingestNamesLine(String line) {
        List<String> fields = TaxReader.readTabPipeLine(line);
        Text taxID =         formatTaxID(fields.get(0));     // 0002077
        String thename = fields.get(1);
        String nametype = fields.get(3);

        if (nametype.equals(SCINAME)) {
            Text col = new Text("sciname|"+thename);
            d4mtw.ingestRow(taxID, col);

        } else {
            Text col = new Text("othername|"+thename+"|"+nametype);
            if (thename.indexOf('|') != -1)
                log.warn("warning: name "+thename+" contains bad character \"|\"");
            d4mtw.ingestRow(taxID, col);
        }
    }

}
