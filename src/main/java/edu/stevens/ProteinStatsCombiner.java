package edu.stevens;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Any protein sequences you insert into a row,column with a ProteinStatsCombiner will add to the stats:
 * a list of the total frequency of each protein
 */
public class ProteinStatsCombiner extends Combiner {

    public static SortedMap<Character,Long> parseAminoMap(String s) {
        assert s.charAt(0)=='{' : "bad input to parseMap";
        SortedMap<Character,Long> m = new TreeMap<>();
        for (String kv : s.substring(1,s.length()-1).split(", ")) {
            Character c = Character.toUpperCase(kv.charAt(0));
            Long l = Long.parseLong(kv.substring(2));
            m.put(c,l);
        }
        //assert m.toString() == s.toString();  // actually, let's allow the input to be in unsorted order
        return m;
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        SortedMap<Character,Long> aminoMap = new TreeMap<>();
        while (iter.hasNext()) {
            String value = iter.next().toString();

            if (value.charAt(0)=='{') { // stats value
                for (String ent : value.substring(1,value.length()-1).split(", ")) {
                    Character c = Character.toUpperCase(ent.charAt(0)); // could eliminate Character.toUpperCase for efficiency...
                    Long l = Long.parseLong(ent.substring(2));
                    Long lin = aminoMap.get(c);
                    if (lin != null)
                        aminoMap.put(c, l + lin);
                }
            } else {
                // read a sequence
                for (char cc : value.toCharArray()) {
                    Character c = Character.toUpperCase(cc);
                    Long lin = aminoMap.get(c);
                    if (lin != null)
                        aminoMap.put(c, 1l + lin);
                    else
                        aminoMap.put(c,1l);
                }
            }
        }
        String ret = aminoMap.toString();
        return new Value(ret.getBytes());
    }

//    @Override
//    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
//        super.init(source, options, env);
//    }

//    @Override
//    public OptionDescriber.IteratorOptions describeOptions() {
//        OptionDescriber.IteratorOptions io = super.describeOptions();
////        io.setName("statsCombiner");
////        io.setDescription("Combiner that keeps track of min, max, sum, and count");
////        io.addNamedOption(RADIX_OPTION, "radix/base of the numbers");
//        return io;
//    }
//
//    @Override
//    public boolean validateOptions(Map<String,String> options) {
//        if (!super.validateOptions(options))
//            return false;
//
////        if (options.containsKey(RADIX_OPTION) && !options.get(RADIX_OPTION).matches("\\d+"))
////            throw new IllegalArgumentException("invalid option " + RADIX_OPTION + ":" + options.get(RADIX_OPTION));
//
//        return true;
//    }

//    /**
//     * A convenience method for setting the expected base/radix of the numbers
//     *
//     * @param iterConfig
//     *          Iterator settings to configure
//     * @param base
//     *          The expected base/radix of the numbers.
//     */
//    public static void setRadix(IteratorSetting iterConfig, int base) {
//        iterConfig.addOption(RADIX_OPTION, base + "");
//    }
}
