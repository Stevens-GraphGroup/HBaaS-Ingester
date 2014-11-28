package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.biojava3.core.sequence.ProteinSequence;
import org.biojava3.core.sequence.io.FastaReaderHelper;

import java.io.File;
import java.io.FilenameFilter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 */
public class MassProteinSeqIngest {
    private ProteinSeqIngest psi;
    //private final Connector connector;

    public MassProteinSeqIngest(Connector connector) {
        //this.connector = connector;
        psi = new ProteinSeqIngest(connector);
    }

    public void insertFile(File file) {
        if (!file.exists() || !file.isFile())
            throw new IllegalArgumentException("please pass a protein sequence file");
        psi.openIngest();
        try { insertFileInner(file); }
        finally {
            psi.closeIngest();
        }
    }

    private void insertFileInner(File file) {
        LinkedHashMap<String, ProteinSequence> proteinData = null;
        try {
            proteinData = FastaReaderHelper.readFastaProteinSequence(file);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        for (Map.Entry<String, ProteinSequence> entry : proteinData.entrySet()) {
            String accID = entry.getKey();
            ProteinSequence ps = entry.getValue();
            psi.putSeq(accID, ps);
        }
    }

    private void insertDirectoryInner(File dir) {
        for (File f : dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".fsa_aa");
            }
        })) {
            insertFileInner(f);
        }
    }

    public void insertDirectory(File dir) {
        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("please pass in a directory");
        psi.openIngest();
        try { insertDirectoryInner(dir); }
        finally {
            psi.closeIngest();
        }
    }
}
