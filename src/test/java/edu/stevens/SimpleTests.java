package edu.stevens;

import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;


public class SimpleTests {

    @Test
    public void testMapReconvert() {
        SortedMap<Character,Long> m = new TreeMap<>();
        m.put('B',1l);
        m.put('A', 3l);
        SortedMap<Character, Long> m2 = ProteinStatsCombiner.parseAminoMap(m.toString());
        assertEquals(m2, m);
    }



}
