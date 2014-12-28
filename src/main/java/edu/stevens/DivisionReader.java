package edu.stevens;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DivisionReader {


    public static Map<Integer, String> readDivisions(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Map<Integer,String> map = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            List<String> li = readLine(line);
            Integer integer = Integer.parseInt(li.get(0));
            String desc = li.get(2);
            map.put(integer, desc);
        }
        return map;
    }

    /** "0\t|\tBCT\t|\tBacteria\t|\t\t|"; */
    //8	|	UNA	|	Unassigned	|	No species nodes should inherit this division assignment	|
    static List<String> readLine(String line) {
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
// \t12
}
