/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ClusteringVariantProcessorTest {

    private ClusteringVariantProcessor clusteringVariantProcessor;

    @Before
    public void setUp() {
        clusteringVariantProcessor = new ClusteringVariantProcessor();
    }

    @Test
    public void processor() {
        List<SubmittedVariant> submittedVariants = createSubmittedVariants();
        long numberOfSubmittedVariants = submittedVariants.size();
        List<SubmittedVariant> clusteredSubmittedVariants = clusteringVariantProcessor.process(submittedVariants);

        long numberOfAccessionAssigned = clusteredSubmittedVariants
                .stream().map(SubmittedVariant::getClusteredVariantAccession).distinct().count();
        assertEquals(4, numberOfAccessionAssigned);

        long numberOfClusteredSubmittedVariants = clusteredSubmittedVariants
                .stream().filter(sv -> sv.getClusteredVariantAccession() != null).count();
        assertEquals(numberOfSubmittedVariants, numberOfClusteredSubmittedVariants);
    }

    private List<SubmittedVariant> createSubmittedVariants() {
        List<SubmittedVariant> submittedVariants = new ArrayList<>();
        SubmittedVariant submittedVariant1 = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                    "T", "A");
        //Different alleles
        SubmittedVariant submittedVariant2 = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                    "T", "G");
        //Same assembly, contig, start but different type
        SubmittedVariant submittedVariantINS = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                      "", "T");
        SubmittedVariant submittedVariantDEL = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                      "A", "");
        //Different assembly, contig and start
        SubmittedVariant submittedVariant3 = createSubmittedVariant("assembly3", 3000, "project3", "contig3", 3000L,
                                                                    "C", "G");
        submittedVariants.add(submittedVariant1);
        submittedVariants.add(submittedVariant2);
        submittedVariants.add(submittedVariantINS);
        submittedVariants.add(submittedVariantDEL);
        submittedVariants.add(submittedVariant3);
        return submittedVariants;
    }

    private SubmittedVariant createSubmittedVariant(String referenceSequenceAccession, int taxonomyAccession,
                                                    String projectAccession, String contig, long start,
                                                    String referenceAllele, String alternateAllele) {
        return new SubmittedVariant(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
                                    referenceAllele, alternateAllele, null, false, false, false, true, null);
    }
}