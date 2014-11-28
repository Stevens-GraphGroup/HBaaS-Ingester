package edu.stevens;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import java.io.File;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String username = "root";
    private static final String password = "secret";
    private static final ClientConfiguration cconfig;
    static {
        String instance = "instance";
        String host = "localhost";
        int timeout = 10000;
        cconfig = ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(host).withZkTimeout(timeout);
    }

    public static void ingestProteins() throws AccumuloSecurityException, AccumuloException {
        Instance instance = new ZooKeeperInstance(cconfig.get(ClientConfiguration.ClientProperty.INSTANCE_NAME), cconfig.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        Connector conn = instance.getConnector(username, new PasswordToken(password));
        File dir = new File("/data/NCBI-ASN1-PROTEIN-FASTA/ftp.ncbi.nih.gov/ncbi-asn1/protein_fasta");
        MassProteinSeqIngest ingest = new MassProteinSeqIngest(conn);
        ingest.insertDirectory(dir);
    }

    public static void main( String[] args ) throws AccumuloSecurityException, AccumuloException {
        ingestProteins();
    }
}
