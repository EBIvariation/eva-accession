/*
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantModelSummaryFunction;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.persistence.ImportedSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.test.MongoTestConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class ImportedSubmittedVariantWriterTest {
    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final String CONTIG_1 = "contig_1";

    private static final String CONTIG_2 = "contig_2";

    private static final int START_1 = 100;

    private static final int START_2 = 200;

    private static final String ALTERNATE_ALLELE = "T";

    private static final String REFERENCE_ALLELE = "A";

    private static final int ACCESSION_COLUMN = 2;

    private static final String ACCESSION_PREFIX = "ss";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private ImportedSubmittedVariantWriter importedSubmittedVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<ISubmittedVariant, String> hashingFunction;

    @Before
    public void setUp() throws Exception {
        importedSubmittedVariantWriter = new ImportedSubmittedVariantWriter();
        hashingFunction = new SubmittedVariantModelSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Test
    public void saveSingleAccession() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                                 "reference", "alternate",
                                                                 CLUSTERED_VARIANT, false, MATCHES_ASSEMBLY);
        ImportedSubmittedVariantEntity variant = new ImportedSubmittedVariantEntity(EXPECTED_ACCESSION,
                                                                                    hashingFunction.apply(submittedVariant),
                                                                                    submittedVariant);

        importedSubmittedVariantWriter.write(Collections.singletonList(variant));

        List<ImportedSubmittedVariantEntity> accessions = mongoTemplate.find(new Query(), ImportedSubmittedVariantEntity.class);
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.iterator().next().getAccession());

        assertVariantEquals(variant, accessions.iterator().next());
    }

    private void assertVariantEquals(ISubmittedVariant expectedvariant, ISubmittedVariant actualVariant) {
        assertEquals(expectedvariant.getAssemblyAccession(), actualVariant.getAssemblyAccession());
        assertEquals(expectedvariant.getTaxonomyAccession(), actualVariant.getTaxonomyAccession());
        assertEquals(expectedvariant.getProjectAccession(), actualVariant.getProjectAccession());
        assertEquals(expectedvariant.getContig(), actualVariant.getContig());
        assertEquals(expectedvariant.getStart(), actualVariant.getStart());
        assertEquals(expectedvariant.getReferenceAllele(), actualVariant.getReferenceAllele());
        assertEquals(expectedvariant.getAlternateAllele(), actualVariant.getAlternateAllele());
        assertEquals(expectedvariant.getClusteredVariant(), actualVariant.getClusteredVariant());
        assertEquals(expectedvariant.isSupportedByEvidence(), actualVariant.isSupportedByEvidence());
        assertEquals(expectedvariant.getMatchesAssembly(), actualVariant.getMatchesAssembly());
    }

}
