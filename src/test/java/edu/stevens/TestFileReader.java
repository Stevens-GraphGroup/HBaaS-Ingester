package edu.stevens;

import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

/**
 * An {@link org.junit.rules.ExternalResource} that loads a file store in the path src/test/resources.
 */
public class TestFileReader extends ExternalResource {
    public static File getTestFile(String filename) {
        URL url = TestFileReader.class.getResource('/' + filename);
        return new File(url.getFile());
    }


    private String filename;
    private File file;
    private FileReader fr;

    public TestFileReader(String filename) {
        super();
        this.filename = filename;
    }

    public FileReader getFileReader() { return fr; }

    @Override
    protected void before() throws Throwable {
        file = getTestFile(filename);
        fr = new FileReader(file);
    }

    @Override
    protected void after() {
        try {
            fr.close();
        } catch (IOException e) {
            System.err.println("Failed to close "+filename+":");
            e.printStackTrace();
        }
    }
}
