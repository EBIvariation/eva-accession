package uk.ac.ebi.eva.accession.pipeline.io;/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;

public class AccessionSummaryWriterTest {

    private static final String CONTIG = "contig";

    private static final int START = 100;

    private static final String REFERENCE = "reference";

    private static final String ALTERNATE = "alternate";

    private AccessionSummaryWriter accessionWriter;

    @Rule
    private TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    private File output;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        accessionWriter = new AccessionSummaryWriter(output, new FastaSequenceReader(Paths.get("mock.fa")));
    }

    @Test
    public void writeVariantWithAccession() {
        ISubmittedVariant variant =
                // TODO: change to SubmittedVariant
                new SubmittedVariantEntity(null, null, "accession", "taxonomy", "project", CONTIG, START, REFERENCE,
                                           ALTERNATE, false);

        accessionWriter.write(Collections.singletonMap(100L, variant));

        assertEquals(String.join("\t", CONTIG, Integer.toString(START), "rs0", REFERENCE, ALTERNATE),
                     getFirstVariantLine());
    }

    private String getFirstVariantLine() {
        output.

    }
}
