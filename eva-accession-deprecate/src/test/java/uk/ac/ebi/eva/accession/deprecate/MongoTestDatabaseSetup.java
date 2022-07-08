/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate;

import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MongoTestDatabaseSetup {

    public static final String ASSEMBLY = "GCA_000000001.1";
    public static final String STUDY1 = "study1";
    public static final String STUDY2 = "study2";
    private static final int TAXONOMY = 60711;
    private static final String REASON = "DEPRECATION_REASON";
    private static SubmittedVariantEntity ss1, ss2, ss3, ss4;
    private static ClusteredVariantEntity rs1, rs2;

    public static SubmittedVariantEntity createSS(String study, Long ssAccession, Long rsAccession, Long start,
                                            String reference, String alternate) {

        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY, TAXONOMY,
                                          study, "chr1", start, reference, alternate, rsAccession, false, false, false,
                                          false, 1);
    }

    public static ClusteredVariantEntity createRS(SubmittedVariantEntity sve) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant cv = new ClusteredVariant(sve.getReferenceSequenceAccession(), sve.getTaxonomyAccession(),
                                                   sve.getContig(),
                                                   sve.getStart(),
                                                   new Variant(sve.getContig(), sve.getStart(), sve.getStart(),
                                                               sve.getReferenceAllele(),
                                                               sve.getAlternateAllele()).getType(),
                                                   true, null);
        String hash = hashingFunction.apply(cv);
        return new ClusteredVariantEntity(sve.getClusteredVariantAccession(), hash, cv);
    }

    public static void populateTestDB(MongoTemplate mongoTemplate) {
        // rs1 -> ss1,ss2
        // rs2 -> ss3,ss4
        ss1 = createSS(STUDY1, 5L, 1L, 100L, "C", "T");
        ss2 = createSS(STUDY1, 6L, 1L, 100L, "C", "A");
        ss3 = createSS(STUDY1, 7L, 5L, 102L, "T", "G");
        ss4 = createSS(STUDY2, 8L, 5L, 102L, "T", "A");

        rs1 = createRS(ss1);
        mongoTemplate.save(rs1, mongoTemplate.getCollectionName(DbsnpClusteredVariantEntity.class));

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3, ss4), SubmittedVariantEntity.class);
        rs2 = createRS(ss3);
        mongoTemplate.save(rs2, mongoTemplate.getCollectionName(ClusteredVariantEntity.class));
    }

    public static void assertPostDeprecationDatabaseState(MongoTemplate mongoTemplate) {
        // ss4 was not deprecated and still remains
        assertEquals(1, mongoTemplate.findAll(SubmittedVariantEntity.class).size());
        assertEquals(3, mongoTemplate.findAll(SubmittedVariantOperationEntity.class).size());
        SubmittedVariantOperationEntity ss1DeprecationOp =
                mongoTemplate.findById("SS_DEPRECATED_TEST_hash5", SubmittedVariantOperationEntity.class);
        SubmittedVariantOperationEntity ss2DeprecationOp =
                mongoTemplate.findById("SS_DEPRECATED_TEST_hash6", SubmittedVariantOperationEntity.class);
        SubmittedVariantOperationEntity ss3DeprecationOp =
                mongoTemplate.findById("SS_DEPRECATED_TEST_hash7", SubmittedVariantOperationEntity.class);
        assertNotNull(ss1DeprecationOp);
        assertNotNull(ss2DeprecationOp);
        assertNotNull(ss3DeprecationOp);
        assertEquals(REASON, ss1DeprecationOp.getReason());
        assertEquals(ss1, ss1DeprecationOp.getInactiveObjects().get(0).toSubmittedVariantEntity());
        assertEquals(REASON, ss2DeprecationOp.getReason());
        assertEquals(ss2, ss2DeprecationOp.getInactiveObjects().get(0).toSubmittedVariantEntity());
        assertEquals(REASON, ss3DeprecationOp.getReason());
        assertEquals(ss3, ss3DeprecationOp.getInactiveObjects().get(0).toSubmittedVariantEntity());

        // Ensure that only the RS with accession 1 (rs1) is deprecated
        // because the RS with accession 5 (rs2) is still associated with ss4
        assertEquals(0, mongoTemplate.findAll(DbsnpClusteredVariantEntity.class).size());
        assertEquals(1, mongoTemplate.findAll(ClusteredVariantEntity.class).size());
        assertEquals(1, mongoTemplate.findAll(DbsnpClusteredVariantOperationEntity.class).size());
        assertEquals(0, mongoTemplate.findAll(ClusteredVariantOperationEntity.class).size());
        DbsnpClusteredVariantOperationEntity rs1DeprecationOp =
                mongoTemplate.findById("RS_DEPRECATED_TEST_" + rs1.getHashedMessage(),
                                            DbsnpClusteredVariantOperationEntity.class);
        assertNotNull(rs1DeprecationOp);
        assertEquals(rs1, rs1DeprecationOp.getInactiveObjects().get(0).toClusteredVariantEntity());
    }
}
