package edu.stevens;

import org.biojava3.core.sequence.ProteinSequence;
import org.biojava3.core.sequence.io.FastaReaderHelper;
import org.biojava3.core.sequence.io.GenbankReaderHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;


public class SimpleTests {

    @Test
    public void testMapReconvert() {
        SortedMap<Character, Long> m = new TreeMap<>();
        m.put('B', 1l);
        m.put('A', 3l);
        SortedMap<Character, Long> m2 = ProteinStatsCombiner.parseAminoMap(m.toString());
        assertEquals(m2, m);
    }


    @Test
    public void testReadGenbankProteinFasta() {
        LinkedHashMap<String, ProteinSequence> sequenceMap = null;
        try {
            // use this if in native Genbank format:
            // GenbankReaderHelper.readGenbankProteinSequence(TestFileReader.getTestFile("gbpri1_cut.fsa_aa"));
            sequenceMap = FastaReaderHelper.readFastaProteinSequence(TestFileReader.getTestFile("gbpri1_cut.fsa_aa"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error reading inpur protein FASTA file: " + "gbpri1_cut.fsa_aa");
        }

        int i = 1;
        for (Map.Entry<String, ProteinSequence> entry : sequenceMap.entrySet()) {
            ProteinSequence ps = entry.getValue();
            System.out.println("Entry " + i + " " + entry.getKey()
//                            + "\n\t" + ps.getSource()   // null
//                            + "\n\t" + ps.getTaxonomy() // null
//                            + "\n\t" + ps.getDescription() // null
                            + "\n\t" + ps.getOriginalHeader()
                            + "\n\t" + ps.getAccession() // BAC05839.1
//                            + "\n\t" + ps.getAnnotationType() // UNKNOWN
                            + "\n\t" + ps.getSequenceAsString()
            );
            String header = ps.getOriginalHeader();
            int splitSpace = header.indexOf(' ');
            String seqID = splitSpace == -1 ? header : header.substring(0, splitSpace);

            // gi|21928500|dbj|BAC05839.1|
            // gi identifier number 21928500
            //
            // accession number BAC05839.1
            String[] parts = seqID.split("\\|");
            Assert.assertEquals("gi",parts[0]);
            String giID = parts[0] + '|'+parts[1];
            String dbID = parts[2]+'|'+parts[3];
            String ascID = parts[3];
            System.out.println("\t\tdbID     :"+giID);
            System.out.println("\t\torgID    :"+dbID);
            System.out.println("\t\tgenbankID:"+ascID);

            String desc = header.substring(splitSpace + 1);
            System.out.println("\t\t" + seqID);
            System.out.println("\t\t" + desc);
            i++;
        }

    }


}
