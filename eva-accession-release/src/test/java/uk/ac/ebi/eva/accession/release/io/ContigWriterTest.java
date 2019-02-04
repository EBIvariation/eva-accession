package uk.ac.ebi.eva.accession.release.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ContigWriterTest {

    private File output;

    private ContigWriter contigWriter;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        contigWriter = new ContigWriter(output);
    }

    @Test
    public void write() throws Exception {
        contigWriter.open(null);
        contigWriter.write(Arrays.asList("CM0001.1", "CM0001.2", "CM0001.3"));
        contigWriter.close();

        assertEquals(3, numberOfLines(output));
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }
}