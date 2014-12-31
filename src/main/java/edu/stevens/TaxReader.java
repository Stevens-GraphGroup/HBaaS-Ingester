package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

public class TaxReader {
    private static final Logger log = LogManager.getLogger(TaxReader.class);
    private final Connector connector;
    private final D4MTableWriter d4mtw;
    private final TableWriter twTDeg, twDeg;
    public static final Text DEG_CHILD_TAX = new Text("degChildTax");
    private boolean isClosed = false;

    public TaxReader(Connector connector) {
        this.connector = connector;
        D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
        config.baseName = "Ttax";
        config.useTable = config.useTableT = true;
        config.useTableDeg = config.useTableTDeg = false;
        config.connector = connector;
        d4mtw = new D4MTableWriter(config);
        twTDeg = new TableWriter("TtaxTDeg", connector, D4MTableWriter.makeDegreeATC()); //TableWriter.EMPTYCF, DEG_CHILD_TAX
        twDeg = new TableWriter("TtaxDeg", connector, D4MTableWriter.makeDegreeATC());
    }

    public void close() {
        isClosed = true;
        d4mtw.closeIngest();
        twTDeg.closeIngest();
        twDeg.closeIngest();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
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
        reader.close();
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

    public void ingestNodesFile(Map<Integer, String> divInfo, File file) throws IOException {
        if (isClosed)
            throw new IllegalStateException(this+" isClosed");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            ingestNodesLine(divInfo, line);
            count++;
        }
        reader.close();
        d4mtw.flushBuffers();
        twTDeg.flushBuffer();
        log.info("TaxReader: ingestNodesFile: ingested "+count+" taxIDs in file "+file.getName());
    }

    public void ingestNamesFile(File file) throws IOException {
        if (isClosed)
            throw new IllegalStateException(this+" isClosed");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        long count = 0;
        int prevTaxInt = -1;
        while ((line = reader.readLine()) != null) {
            prevTaxInt = ingestNamesLine(prevTaxInt, line);
            count++;
        }
        reader.close();
        d4mtw.flushBuffers();
        twDeg.flushBuffer();
        log.info("TaxReader: ingestNamesFile: ingested "+count+" names in file "+file.getName());
    }

    private void ingestNodesLine(Map<Integer, String> divInfo, String line) {
        List<String> fields = readTabPipeLine(line);
        Text taxID =         formatTaxID(fields.get(0));     // 0002077
        Text parentTaxID =   formatTaxID(fields.get(1));     // 0186821
        Text rank =          new Text("rank|"+fields.get(2) );         // genus
        Text divisionDesc =  new Text("division|"+divInfo.get(Integer.parseInt(fields.get(4))));

        if (!taxID.toString().equals("1")) {
            d4mtw.ingestRow(taxID, parentTaxID);
            twTDeg.ingestRow(parentTaxID, DEG_CHILD_TAX);
        }
        d4mtw.ingestRow(taxID, rank);
        d4mtw.ingestRow(taxID, divisionDesc);
    }

    public static final int PADLENGTH_taxid = 7;

    static Text formatTaxID(String taxID) {
        String padded = StringUtils.leftPad(taxID, PADLENGTH_taxid, '0');  // pad to 9 digits
        return new Text("taxid|"+padded);
    }


    static final String SCINAME = "scientific name";
    private static final Text DEG_ROW_TAX = new Text("taxid");

    // 7	|	Azorhizobium caulinodans	|		|	scientific name	|
    // 7	|	Azorhizobium caulinodans Dreyfus et al. 1988	|		|	synonym	|
    /** Passes in the taxID as an int of the previous line. If we moved onto a new taxID line, then increment the degree counter. */
    private int ingestNamesLine(int prevTaxInt, String line) {
        List<String> fields = TaxReader.readTabPipeLine(line);
        String taxString = fields.get(0);
        int taxInt = Integer.parseInt(taxString);
        Text taxID =         formatTaxID(taxString);     // 0002077
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

        if (taxInt != prevTaxInt) {
            twDeg.ingestRow(DEG_ROW_TAX, D4MTableWriter.DEFAULT_DEGCOL);
        }
        return taxInt;
    }



}
