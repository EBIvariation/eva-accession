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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Test;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;

import java.util.Arrays;
import java.util.List;

public class ContigSynonymValidationProcessorTest {

    @Test
    public void allContigsHaveSynonyms() throws Exception {
        String fileString = ContigSynonymValidationProcessorTest.class.getResource(
                "/input-files/assembly-report/GCA_000001635.8_Mus_musculus-grcm38.p6_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);
        ContigSynonymValidationProcessor processor = new ContigSynonymValidationProcessor(contigMapping);
        List<String> contigsInDb = Arrays.asList("chrom1", "2", "NT_166280.1");
        for (String contig : contigsInDb) {
            processor.process(contig);
        }
    }
}