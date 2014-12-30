package edu.stevens;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Example tests.
 */
public class MiniTest {


//    public static Test suite()
//    {
//        return new TestSuite( MiniTest.class );
//    }

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = new MiniInstance();

    @Test
    public void testOne()
    {
        System.out.println("name of instance: "+tester.getInstance().getInstanceName());
        assertTrue(true);
    }


    @Test
    public void testInsertScan() throws Exception {
        final String tableName = "testTable";
        Connector conn = tester.getInstance().getConnector(tester.getUser(), new PasswordToken(tester.getPassword()));
        // create tableName
        if (!conn.tableOperations().exists(tableName))
            conn.tableOperations().create(tableName);

        // make a table split in tableName
        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("f"));
        conn.tableOperations().addSplits(tableName, splitset);


        // write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text[] rows = new Text[] {new Text("ccc"), new Text("ddd"), new Text("pogo")};
        Text cf = new Text("");
        Text cq = new Text("cq");
        Value v = new Value("7".getBytes());
        for (int i=0; i<rows.length; i++) {
            Mutation m = new Mutation(rows[i]);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

        // read back
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
        Key startKey = new Key(new Text("d"), cf, cq);
        Range rng = new Range(startKey,null,true,false,false,true);
        scan.setRange(rng);// (new Key("d"));
//        System.out.println(tableName+" table:");
        Iterator<Map.Entry<Key, Value>> iterator = scan.iterator();

        Key k2 = new Key(new Text("ddd"), cf, cq);
        Key k3 = new Key(new Text("pogo"), cf, cq);
        Map.Entry<Key, Value> r1 = iterator.next();
        assertTrue(k2.equals(r1.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        assertEquals(v,r1.getValue());
        Map.Entry<Key, Value> r2 = iterator.next();
        assertTrue(k3.equals(r2.getKey(),PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        assertEquals(v,r2.getValue());
        assertFalse(iterator.hasNext());

//        for (Map.Entry<Key, Value> kv : scan) {
//            System.out.println(kv);
//        }
        conn.tableOperations().delete(tableName);
    }

    @Test
    public void testProteinStatsCombiner() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        final String tableName = "testTableProteinStats";
        Connector conn = tester.getInstance().getConnector(tester.getUser(), new PasswordToken(tester.getPassword()));
        // create tableName
        if (!conn.tableOperations().exists(tableName))
            conn.tableOperations().create(tableName);

        // write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text row = new Text("sp|Q6GZX4|001R_FRG3G");
        Text cf = new Text("cf");
        Text cq = new Text("cq");
        Value v = new Value("MAFSAEDVLKEYDRRrRMEALLLSL".getBytes());
        Mutation m = new Mutation(row);
        m.put(cf, cq, v);
        writer.addMutation(m);
        writer.flush();

        // read back
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
        //Key startKey = new Key(new Text("d"), cf, cq);
        //Range rng = new Range(null,null,true,false,true,true);
        //scan.setRange(rng);// (new Key("d"));
//        System.out.println(tableName+" table:");
        Iterator<Map.Entry<Key, Value>> iterator = scan.iterator();

        Key k1 = new Key(row, cf, cq);
        Map.Entry<Key, Value> r1 = iterator.next();
        assertTrue(k1.equals(r1.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        assertEquals(v,r1.getValue());

        // set iterator
        String iterName = "proteinStatsCombiner";
        // Setup IteratorSetting
        IteratorSetting cfg = new IteratorSetting(5, iterName, ProteinStatsCombiner.class);
        Combiner.setColumns(cfg, Collections.singletonList(new IteratorSetting.Column(cf, cq)));
        Combiner.setCombineAllColumns(cfg, false);
        // Add Iterator to table
        conn.tableOperations().attachIterator(tableName, cfg);
        // Verify successful add
        Map<String,EnumSet<IteratorUtil.IteratorScope>> iterMap = conn.tableOperations().listIterators(tableName);
        EnumSet<IteratorUtil.IteratorScope> iterScope = iterMap.get(iterName);
        Assert.assertNotNull(iterScope);
        Assert.assertTrue(iterScope.containsAll(EnumSet.allOf(IteratorUtil.IteratorScope.class)));

        // read back
        iterator = scan.iterator();
        Map.Entry<Key, Value> r2 = iterator.next();
        assertTrue(k1.equals(r2.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        //System.out.println("HERE");
//        System.out.println(r2.getValue());
//        System.out.println(ProteinStatsCombiner.parseAminoMap(r2.getValue().toString()));
        assertEquals("{A=3, D=2, E=3, F=1, K=1, L=5, M=2, R=4, S=2, V=1, Y=1}", r2.getValue().toString());
        assertFalse(iterator.hasNext());

        conn.tableOperations().delete(tableName);
    }

    @Test
    public void testInsertProteinFile() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        File f = TestFileReader.getTestFile("gbpri1_cut.fsa_aa");
        Connector conn = tester.getInstance().getConnector(tester.getUser(), new PasswordToken(tester.getPassword()));
        MassProteinSeqIngest ingest = new MassProteinSeqIngest(conn);
        ingest.insertFile(f);

        // read back
        Scanner scan = conn.createScanner("TseqRaw", Authorizations.EMPTY);
//        Key startKey = new Key(new Text("d"), cf, cq);
//        Range rng = new Range(startKey,null,true,false,false,true);
//        scan.setRange(rng);// (new Key("d"));
//        System.out.println(tableName+" table:");
//        Iterator<Map.Entry<Key, Value>> iterator = scan.iterator();
//        Key k2 = new Key(new Text("ddd"), cf, cq);
//        Key k3 = new Key(new Text("pogo"), cf, cq);
//        Map.Entry<Key, Value> r1 = iterator.next();
//        assertTrue(k2.equals(r1.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
//        assertEquals(v,r1.getValue());
//        Map.Entry<Key, Value> r2 = iterator.next();
//        assertTrue(k3.equals(r2.getKey(),PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
//        assertEquals(v,r2.getValue());
//        assertFalse(iterator.hasNext());

        System.out.println("TseqRaw:  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();
        scan = conn.createScanner("Tseq",Authorizations.EMPTY);
        System.out.println("Tseq   :  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();
        scan = conn.createScanner("TseqT",Authorizations.EMPTY);
        System.out.println("TseqT  :  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();
        scan = conn.createScanner("TseqTDeg",Authorizations.EMPTY);
        System.out.println("TseqTDeg  :  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();

        conn.tableOperations().delete("TseqRaw");
        conn.tableOperations().delete("Tseq");
        conn.tableOperations().delete("TseqT");
        conn.tableOperations().delete("TseqTDeg");
    }

    @Test
    public void testInsertNodeData() throws AccumuloSecurityException, AccumuloException, IOException, TableNotFoundException {
        File f = TestFileReader.getTestFile("nodes_cut.dmp");
        Connector conn = tester.getInstance().getConnector(tester.getUser(), new PasswordToken(tester.getPassword()));
        Map<Integer, String> divMap = TaxReader.readDivisions(TestFileReader.getTestFile("division.dmp"));
        TaxReader nr = new TaxReader(divMap, conn);
        nr.ingestNodesFile(f);

        // read back
        Scanner scan = conn.createScanner("Ttax", Authorizations.EMPTY);
        System.out.println("Ttax:  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();
        scan = conn.createScanner("TtaxT", Authorizations.EMPTY);
        System.out.println("TtaxT:  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();

        conn.tableOperations().delete("Ttax");
        conn.tableOperations().delete("TtaxT");
    }

    @Test
    public void testInsertNameData() throws AccumuloSecurityException, AccumuloException, IOException, TableNotFoundException {
        File f = TestFileReader.getTestFile("names_cut.dmp");
        Connector conn = tester.getInstance().getConnector(tester.getUser(), new PasswordToken(tester.getPassword()));
        Map<Integer, String> divMap = TaxReader.readDivisions(TestFileReader.getTestFile("division.dmp"));
        TaxReader nr = new TaxReader(divMap, conn);
        nr.ingestNamesFile(f);

        // read back
        Scanner scan = conn.createScanner("Ttax", Authorizations.EMPTY);
        System.out.println("Ttax:  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();
        scan = conn.createScanner("TtaxT", Authorizations.EMPTY);
        System.out.println("TtaxT:  ");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }
        scan.close();

        conn.tableOperations().delete("Ttax");
        conn.tableOperations().delete("TtaxT");
    }

}
