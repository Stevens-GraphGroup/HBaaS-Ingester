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
    public static void ingestProteins(File dir) throws AccumuloSecurityException, AccumuloException {
        log.info("START ingestProteins: "+dir);
        Connector conn = setupAccumuloConnector();
        //File dir = new File("/data/NCBI-ASN1-PROTEIN-FASTA/ftp.ncbi.nih.gov/ncbi-asn1/protein_fasta");
        MassProteinSeqIngest ingest = new MassProteinSeqIngest(conn);
        ingest.insertDirectory(dir);
    }

    /** Need divisions.dmp, nodes.dmp, names.dmp */
    public static void ingestTaxNames(File dir) throws IOException {
        log.info("START ingestTaxNames: "+dir);
        Connector conn = setupAccumuloConnector();

        File fDiv = new File(dir, "division.dmp");
        if (!fDiv.exists())
            throw new FileNotFoundException("division.dmp is not in dir: "+dir);
        File fNodes = new File(dir, "nodes.dmp");
        if (!fNodes.exists())
            throw new FileNotFoundException("nodes.dmp is not in dir: "+dir);
        File fNames = new File(dir, "names.dmp");
        if (!fNames.exists())
            throw new FileNotFoundException("names.dmp is not in dir: "+dir);

        Map<Integer, String> divMap = DivisionReader.readDivisions(fDiv);
        log.info("ingestTaxNames: finished division.dmp");

        NodesReader nr = new NodesReader(divMap, conn);
        nr.ingestFile(fNodes);
        log.info("ingestTaxNames: finished nodes.dmp");

        throw new RuntimeException("not yet implemented");

        //log.info("ingestTaxNames: finished names.dmp");
    }

    /** Need gi_taxid_prot.dmp */
    public static void ingestTaxLink(File dir) {
        log.info("START ingestTaxLink: "+dir);
        Connector conn = setupAccumuloConnector();
        throw new RuntimeException("not yet implemented");
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
        Option o1 = new Option("w", true, "what to do (ingestProteins,ingestTaxNames,ingestTaxLink)");
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
        for(String w : line.getOptionValues('w')) {
            switch(w) {
                case "ingestProteins":
                    ingestProteins(fprot);
                    break;
                case "ingestTaxNames":
                    ingestTaxNames(ftax);
                    break;
                case "ingestTaxLink":
                    ingestTaxLink(ftax);
                    break;
                default:
                    failHelp("bad option: "+w, options);
            }
        }
    }
}
