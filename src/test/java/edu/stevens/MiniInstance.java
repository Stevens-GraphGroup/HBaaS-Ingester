package edu.stevens;

import junit.framework.TestCase;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * Provides a MiniAccumuloCluster for use in testing.
 */
public class MiniInstance extends ExternalResource implements IAccumuloTester
{
    /* Fixture State */
    private File tempDir;
    private MiniAccumuloCluster miniaccumulo;
    private Instance instance;
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    public String getUser() {
        return USER;
    }

    public String getPassword() {
        return PASSWORD;
    }

    public Instance getInstance() {
        return instance;
    }

    @Override
    protected void before() throws Throwable {
        tempDir = Files.createTempDirectory("tempMini",new FileAttribute<?>[] {}).toFile();
        miniaccumulo = new MiniAccumuloCluster(tempDir, PASSWORD);
        miniaccumulo.start();
        instance = new ZooKeeperInstance(miniaccumulo.getInstanceName(), miniaccumulo.getZooKeepers());
        System.out.println("setUp ok - instance: " + instance.getInstanceName());
    }

    @Override
    protected void after() {
        instance = null;
        try {
            miniaccumulo.stop();
        } catch (IOException | InterruptedException e) {
            System.err.print("Error stopping MiniAccumuloCluster: ");
            e.printStackTrace();
        }
        tempDir.delete();
        System.out.println("tearDown ok - instance destroyed");
    }
}
