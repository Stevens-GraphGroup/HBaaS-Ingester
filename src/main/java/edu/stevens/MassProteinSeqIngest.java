package edu.stevens;

import org.apache.accumulo.core.client.Connector;
import org.biojava3.core.sequence.ProteinSequence;
import org.biojava3.core.sequence.compound.AminoAcidCompound;
import org.biojava3.core.sequence.compound.AminoAcidCompoundSet;
import org.biojava3.core.sequence.io.FastaReader;
import org.biojava3.core.sequence.io.FastaReaderHelper;
import org.biojava3.core.sequence.io.GenericFastaHeaderParser;
import org.biojava3.core.sequence.io.ProteinSequenceCreator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Insert all the protein sequences in a file or folder.
 * Needs updating in light of Matlab D4M.
 */
@Deprecated
public class MassProteinSeqIngest {
    private ProteinSeqIngest psi;
    //private final Connector connector;
    private long seqsIngested = 0l;
    public long getSeqsIngested() {
        return seqsIngested;
    }

    public MassProteinSeqIngest(Connector connector) {
        //this.connector = connector;
        psi = new ProteinSeqIngest(connector);
    }

    public void insertFile(File file) {
        if (!file.exists() || !file.isFile())
            throw new IllegalArgumentException("please pass a protein sequence file (can be gzipped)");
//        psi.openIngest();  This is done automatically.
        try { insertFileInner(file); }
        finally {
            psi.closeIngest();
        }
    }

    private void insertFileInner(File file) {
        LinkedHashMap<String, ProteinSequence> proteinData = null;
        try {
            if (file.getName().endsWith(".gz")) {
                FastaReader<ProteinSequence, AminoAcidCompound> fr =
                        new FastaReader<>(new GZIPInputStream(new FileInputStream(file)),
                            new GenericFastaHeaderParser<ProteinSequence, AminoAcidCompound>(),
                            new ProteinSequenceCreator(AminoAcidCompoundSet.getAminoAcidCompoundSet()));
                proteinData = fr.process();
                fr.close();
            } else {
                proteinData = FastaReaderHelper.readFastaProteinSequence(file);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        long seqsIngestedFile = 0l;
        for (Map.Entry<String, ProteinSequence> entry : proteinData.entrySet()) {
            String accID = entry.getKey();
            ProteinSequence ps = entry.getValue();
            psi.putSeq(accID, ps);
            seqsIngestedFile++;
            seqsIngested++;
        }
        psi.flushBuffers();
        System.out.println("finished ingesting "+seqsIngestedFile+" seqs in file: "+file.getName());
    }

    private void insertDirectoryInner(File dir) {
        for (File f : dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".fsa_aa") || name.endsWith(".fsa_aa.gz"); // accept gzip files
            }
        })) {
            insertFileInner(f);
        }
        System.out.println("ingest seq count: "+seqsIngested);
        System.out.println("in directory:"+dir.getAbsolutePath());
    }

    public void insertDirectory(File dir) {
        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("please pass in a directory");
//        psi.openIngest();
        try { insertDirectoryInner(dir); }
        finally {
            psi.closeIngest();
        }
    }
}
