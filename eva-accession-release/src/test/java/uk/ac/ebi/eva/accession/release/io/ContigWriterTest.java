package uk.ac.ebi.eva.accession.release.io;


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

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
        contigWriter.write(Collections.singletonList("<ID=CM0001.1>"));
        contigWriter.close();
    }
}