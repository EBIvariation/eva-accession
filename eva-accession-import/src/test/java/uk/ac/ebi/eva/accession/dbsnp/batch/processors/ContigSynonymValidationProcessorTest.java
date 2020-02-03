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
package uk.ac.ebi.eva.accession.dbsnp.batch.processors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.model.CoordinatesPresence;

import java.util.Arrays;
import java.util.List;

public class ContigSynonymValidationProcessorTest {

    private ContigSynonymValidationProcessor processor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        String fileString = ContigSynonymValidationProcessorTest.class.getResource(
            "/input-files/assembly-report/GCA_000001635.8_Mus_musculus-grcm38.p6_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);
        processor = new ContigSynonymValidationProcessor(contigMapping);
    }

    @Test
    public void allContigsHaveSynonyms() throws Exception {
        List<CoordinatesPresence> contigsInDb = Arrays.asList(new CoordinatesPresence("chrom1", true, "NC_000067.6"),
                                                              new CoordinatesPresence("2", true, "NC_000068.7"),
                                                              new CoordinatesPresence("MMCHR1_RANDOM_CTG1", true,
                                                                                      "NT_166280.1"));
        for (CoordinatesPresence presence : contigsInDb) {
            processor.process(presence);
        }
    }

    @Test
    public void onlyChromosomeSynonymAvailable() throws Exception {
        processor.process(new CoordinatesPresence("2", true, "NT_without_synonym"));
    }

    @Test
    public void onlyContigSynonymAvailable() throws Exception {
        processor.process(new CoordinatesPresence(null, false, "NT_166280.1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonIdenticalContigSynonymWithUnusableChromosome() throws Exception {
        processor.process(new CoordinatesPresence("2", false, "NT_without_synonym"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonIdenticalContigSynonym() throws Exception {
        processor.process(new CoordinatesPresence(null, false, "NT_without_synonym"));
    }

    @Test
    public void identicalAndNonIdenticalSynonyms() throws Exception {
        processor.process(new CoordinatesPresence(null, false, "NT_166280.1"));
        processor.process(new CoordinatesPresence("2", true, "NT_without_synonym"));
        thrown.expect(IllegalArgumentException.class);
        processor.process(new CoordinatesPresence(null, false, "NT_without_synonym"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void contigMissingFromAssemblyReport() throws Exception {
        processor.process(new CoordinatesPresence(null, false, "contig_not_present_in_assembly_report"));
    }
}
