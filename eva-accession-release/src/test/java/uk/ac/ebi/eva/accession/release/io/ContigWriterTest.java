/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.release.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;

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
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms("Chr1", "A", "A", "CM0001.1", "A", "A", true),
                new ContigSynonyms("Chr2", "A", "A", "CM0002.1", "A", "A", true),
                new ContigSynonyms("Chr3", "A", "A", "CM0003.1", "A", "A", true)));
        contigWriter = new ContigWriter(output, contigMapping);
    }

    @Test
    public void write() throws Exception {
        contigWriter.open(null);
        contigWriter.write(Arrays.asList("CM0001.1", "CM0002.2", "CM0003.3"));
        contigWriter.close();

        assertEquals(3, numberOfLines(output));
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }
}