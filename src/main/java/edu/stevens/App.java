package edu.stevens;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger log = LogManager.getLogger(App.class);
    private static final String username = "root";
    private static final String password = "secret";
    private static final ClientConfiguration cconfig;
    static {
        String instance = "instance";
        String host = "localhost";
        int timeout = 10000;
        cconfig = ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(host).withZkTimeout(timeout);
    }

    private static Connector setupAccumuloConnector() {
        Instance instance = new ZooKeeperInstance(cconfig.get(ClientConfiguration.ClientProperty.INSTANCE_NAME), cconfig.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        try {
            return instance.getConnector(username, new PasswordToken(password));
        } catch (AccumuloException | AccumuloSecurityException e) {
            e.printStackTrace();
            System.exit(2);
            return null;
        }
    }

    /** Need NCBI genbank files in dir, some/all can be gunzipped */
    public static void ingestProteins(File dir, Connector conn) throws AccumuloSecurityException, AccumuloException {
        log.info("START ingestProteins: "+dir);
        //File dir = new File("/data/NCBI-ASN1-PROTEIN-FASTA/ftp.ncbi.nih.gov/ncbi-asn1/protein_fasta");
        MassProteinSeqIngest ingest = new MassProteinSeqIngest(conn);
        ingest.insertDirectory(dir);
    }

    /** Need divisions.dmp, nodes.dmp, names.dmp */
    public static void ingestTaxNodesNames(File dir, Connector conn) throws IOException {
        log.info("START  ingestTaxNodesNames: "+dir);

        File fDiv = new File(dir, "division.dmp");
        if (!fDiv.exists())
            throw new FileNotFoundException("division.dmp is not in dir: "+dir);
        File fNodes = new File(dir, "nodes.dmp");
        if (!fNodes.exists())
            throw new FileNotFoundException("nodes.dmp is not in dir: "+dir);
        File fNames = new File(dir, "names.dmp");
        if (!fNames.exists())
            throw new FileNotFoundException("names.dmp is not in dir: "+dir);

        Map<Integer, String> divMap = TaxReader.readDivisions(fDiv);
        log.info("ingestTaxNodesNames: finished division.dmp");

        TaxReader nr = new TaxReader(conn);
        nr.ingestNodesFile(divMap, fNodes);
        nr.ingestNamesFile(fNames);
        nr.close();
        log.info("FINISH ingestTaxNodesNames: "+dir);
    }

    /** Need gi_taxid_prot.dmp */
    public static void ingestTaxLink(File dir, Connector conn) throws IOException {
        log.info("START  ingestTaxLink: "+dir);

        TaxLinkReader tlr = new TaxLinkReader(conn);
        File fLink = new File(dir, "gi_taxid_prot.dmp");
        if (!fLink.exists())
            throw new FileNotFoundException("gi_taxid_prot.dmp is not in dir: "+dir);

        tlr.ingestTaxLinkFile(fLink);
        tlr.close();
        log.info("FINISH ingestTaxLink: "+dir);
    }

    private static File mustGetDirFile(String str) {
        File dir = new File(str);
        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println(str+" is not a directory");
            System.exit(1);
        }
        return dir;
    }

    private static void failHelp(String reason, Options options) {
        System.out.println( "Failure: " + reason );
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "edu.stevens.App", options );
        System.exit(1);
    }

    public static void main( String[] args ) throws AccumuloSecurityException, AccumuloException, IOException {
        Options options = new Options();
        Option o1 = new Option("w", true, "what to do (ingestProteins,ingestTaxNodesNames,ingestTaxLink)");
        o1.setRequired(true);
        o1.setValueSeparator(',');
        options.addOption(o1);
        options.addOption("dprot", true, "path to protein sequence directory");
        options.addOption("dtax", true, "path to taxonomy data directory");

        CommandLineParser parser = new org.apache.commons.cli.GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            failHelp(exp.getMessage(), options);
            return;
        }

        String dprot = line.getOptionValue("dprot", ".");
        String dtax = line.getOptionValue("dtax", ".");
        File fprot = mustGetDirFile(dprot);
        File ftax = mustGetDirFile(dtax);
        Connector conn = setupAccumuloConnector();
        for(String w : line.getOptionValues('w')) {
            switch(w) {
                case "ingestProteins":
                    ingestProteins(fprot, conn);
                    break;
                case "ingestTaxNodesNames":
                    ingestTaxNodesNames(ftax, conn);
                    break;
                case "ingestTaxLink":
                    ingestTaxLink(ftax, conn);
                    break;
                default:
                    failHelp("bad option: "+w, options);
            }
        }
    }
}
