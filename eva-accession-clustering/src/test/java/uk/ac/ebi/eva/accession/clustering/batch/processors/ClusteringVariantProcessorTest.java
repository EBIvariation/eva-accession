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
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class ClusteringVariantProcessorTest {

    private ClusteringVariantProcessor clusteringVariantProcessor;

    private Function<ISubmittedVariant, String> hashingFunction;

    @Before
    public void setUp() {
        clusteringVariantProcessor = new ClusteringVariantProcessor();
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Test
    public void processor() {
        List<SubmittedVariantEntity> submittedVariants = createSubmittedVariantEntities();
        long numberOfSubmittedVariants = submittedVariants.size();
        List<SubmittedVariantEntity> clusteredSubmittedVariants = clusteringVariantProcessor.process(submittedVariants);

        long numberOfAccessionAssigned = clusteredSubmittedVariants
                .stream().map(SubmittedVariantEntity::getClusteredVariantAccession).distinct().count();
        assertEquals(4, numberOfAccessionAssigned);

        long numberOfClusteredSubmittedVariants = clusteredSubmittedVariants
                .stream().filter(sv -> sv.getClusteredVariantAccession() != null).count();
        assertEquals(numberOfSubmittedVariants, numberOfClusteredSubmittedVariants);
    }

    private List<SubmittedVariantEntity> createSubmittedVariantEntities() {
        List<SubmittedVariantEntity> submittedVariantEntities = new ArrayList<>();
        SubmittedVariant submittedVariant1 = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                    "T", "A");
        SubmittedVariantEntity submittedVariantEntity1 = createSubmittedVariantEntity(submittedVariant1);
        //Different alleles
        SubmittedVariant submittedVariant2 = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                    "T", "G");
        SubmittedVariantEntity submittedVariantEntity2 = createSubmittedVariantEntity(submittedVariant2);
        //Same assembly, contig, start but different type
        SubmittedVariant submittedVariantINS = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                      "", "T");
        SubmittedVariantEntity submittedVariantEntityINS = createSubmittedVariantEntity(submittedVariantINS);
        SubmittedVariant submittedVariantDEL = createSubmittedVariant("assembly1", 1000, "project1", "contig1", 1000L,
                                                                      "A", "");
        SubmittedVariantEntity submittedVariantEntityDEL = createSubmittedVariantEntity(submittedVariantDEL);
        //Different assembly, contig and start
        SubmittedVariant submittedVariant3 = createSubmittedVariant("assembly3", 3000, "project3", "contig3", 3000L,
                                                                    "C", "G");
        SubmittedVariantEntity submittedVariantEntity3 = createSubmittedVariantEntity(submittedVariant3);
        submittedVariantEntities.add(submittedVariantEntity1);
        submittedVariantEntities.add(submittedVariantEntity2);
        submittedVariantEntities.add(submittedVariantEntityINS);
        submittedVariantEntities.add(submittedVariantEntityDEL);
        submittedVariantEntities.add(submittedVariantEntity3);
        return submittedVariantEntities;
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(SubmittedVariant submittedVariant) {
        String hash1 = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(1L, hash1, submittedVariant, 1);
        return submittedVariantEntity;
    }

    private SubmittedVariant createSubmittedVariant(String referenceSequenceAccession, int taxonomyAccession,
                                                    String projectAccession, String contig, long start,
                                                    String referenceAllele, String alternateAllele) {
        return new SubmittedVariant(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
                                    referenceAllele, alternateAllele, null, false, false, false, true, null);
    }

    @Test
    public void reader() throws Exception {
        URI vcfUri = ClusteringVariantProcessorTest.class.getResource("/input-files/vcf/aggregated_accessioned.vcf.gz").toURI();
        File vcfFile = new File(vcfUri);
        AggregatedVcfReader vcfReader = new AggregatedVcfReader("fileId", "studyId", Aggregation.BASIC, null, vcfFile);
        vcfReader.open(new ExecutionContext());
        List<Variant> read = vcfReader.read();
        vcfReader.close();
    }
}