package edu.stevens;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.biojava3.core.sequence.ProteinSequence;
import org.biojava3.core.sequence.io.FastaReaderHelper;
import org.biojava3.core.sequence.io.GenbankReaderHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;


public class SimpleTests {
    private static final Logger log = LogManager.getLogger(SimpleTests.class);

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
//            System.out.println("Entry " + i + " " + entry.getKey()
////                            + "\n\t" + ps.getSource()   // null
////                            + "\n\t" + ps.getTaxonomy() // null
////                            + "\n\t" + ps.getDescription() // null
//                            + "\n\t" + ps.getOriginalHeader()
//                            + "\n\t" + ps.getAccession() // BAC05839.1
////                            + "\n\t" + ps.getAnnotationType() // UNKNOWN
//                            + "\n\t" + ps.getSequenceAsString()
//            );
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
//            System.out.println("\t\tgiID     :"+giID);  //gi|21928500
//            System.out.println("\t\tdbID     :"+dbID);  //dbj|BAC05839.1
//            System.out.println("\t\tascID    :"+ascID); //BAC05839.1
            Assert.assertEquals("gi|21928500",giID);
            Assert.assertEquals("dbj|BAC05839.1",dbID);
            Assert.assertEquals("BAC05839.1",ascID);
            Assert.assertEquals("BAC05839.1",ps.getAccession().getID());

            String desc = header.substring(splitSpace + 1).trim(); // remove surrounding whitespace
//            System.out.println("\t\t" + seqID);         //gi|21928500|dbj|BAC05839.1|
//            System.out.println("\t\t" + desc);          //seven transmembrane helix receptor, partial [Homo sapiens]
            i++;
            break; // only do first entry
        }

    }

    @Test
    public void testReadDivision() throws IOException {

        File file = TestFileReader.getTestFile("division.dmp");
        Map<Integer,String> map = DivisionReader.readDivisions(file);
        log.debug("MAP: "+map);
        Assert.assertEquals("Phages", map.get(3));
    }


    @Test
    public void testReadDivisionLine() {
        String s = "0\t|\tBCT\t|\tBacteria\t|\t\t|";
        List<String> list = DivisionReader.readLine(s);
        String[] exp = new String[] {"0", "BCT", "Bacteria", ""};
        Assert.assertEquals(Arrays.asList(exp), list);

        String s2 = "8\t|\tUNA\t|\tUnassigned\t|\tNo species nodes should inherit this division assignment\t|";
        List<String> list2 = DivisionReader.readLine(s2);
        String[] exp2 = new String[] {"8", "UNA", "Unassigned", "No species nodes should inherit this division assignment"};
        Assert.assertEquals(Arrays.asList(exp2), list2);
    }

}
