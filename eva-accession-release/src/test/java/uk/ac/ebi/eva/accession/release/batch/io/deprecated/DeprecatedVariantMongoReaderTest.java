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

package uk.ac.ebi.eva.accession.release.batch.io.deprecated;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DeprecatedVariantMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000001735.1";

    private static final int TAXONOMY_1 = 3702;

    private static final int TAXONOMY_2 = 3703;

    private static final int CHUNK_SIZE = 5;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private DeprecatedVariantMongoReader reader;

    @Before
    public void setUp() {
        this.mongoTemplate.getDb().drop();
    }

    @After
    public void tearDown() {
        this.mongoTemplate.getDb().drop();
        this.reader.close();
    }

    private List<Variant> readIntoList() {
        List<Variant> variants = new ArrayList<>();
        List<Variant> variantsInBatch;

        while ((variantsInBatch = reader.read()) != null) {
            variants.addAll(variantsInBatch);
        }
        return variants;
    }

    @Test
    public void testOnlySpecifiedTaxVariantsRead() {
        // See scenario here: https://docs.google.com/spreadsheets/d/12QJT4N0-UJGTv3BtVq_gyyrVzweXd5ev2WFlnP-4MW4/edit#rangeid=1202280213
        DbsnpSubmittedVariantEntity ss1 = createSS(ASSEMBLY, TAXONOMY_1, 1L, 1L, 100L, "C", "A");
        DbsnpSubmittedVariantEntity ss2 = createSS(ASSEMBLY, TAXONOMY_2, 2L, 2L, 101L, "A", "T");
        DbsnpSubmittedVariantEntity ss3 = createSS(ASSEMBLY, TAXONOMY_2, 3L, 3L, 102L, "T", "G");
        DbsnpClusteredVariantEntity rs1 = createRS(ss1, null);
        DbsnpClusteredVariantEntity rs2 = createRS(ss2, null);
        DbsnpClusteredVariantEntity rs3 = createRS(ss3, TAXONOMY_1);
        this.mongoTemplate.insert(Stream.of(ss1, ss2, ss3).map(ss -> {
            DbsnpSubmittedVariantOperationEntity dbsnpSvoeObj = new DbsnpSubmittedVariantOperationEntity();
            dbsnpSvoeObj.fill( EventType.UPDATED, ss.getAccession(),
                              "Declustered: None of the variant alleles match the reference allele.",
                              Arrays.asList(new DbsnpSubmittedVariantInactiveEntity(ss)));
            return dbsnpSvoeObj;
        }).collect(Collectors.toList()), DbsnpSubmittedVariantOperationEntity.class);
        this.mongoTemplate.insert(Stream.of(rs1, rs2, rs3).map(rs -> {
            DbsnpClusteredVariantOperationEntity dbsnpCvoeObj = new DbsnpClusteredVariantOperationEntity();
            dbsnpCvoeObj.fill( EventType.DEPRECATED, rs.getAccession(),
                               "Clustered variant completely declustered",
                               Arrays.asList(new DbsnpClusteredVariantInactiveEntity(rs)));
            return dbsnpCvoeObj;
        }).collect(Collectors.toList()), DbsnpClusteredVariantOperationEntity.class);

        // Test records returned when the mongo reader is given TAXONOMY_1
        this.reader = new DeprecatedVariantMongoReader(ASSEMBLY, TAXONOMY_1, mongoClient, TEST_DB, CHUNK_SIZE,
                                                  new DbsnpCollectionNames());
        this.reader.open(new ExecutionContext());
        List<Variant> deprecatedVariants = this.readIntoList();
        assertEquals(1, deprecatedVariants.size());
        assertEquals("rs1", deprecatedVariants.get(0).getMainId());

        // Test records returned when the mongo reader is given TAXONOMY_2
        // Note rs3 is returned even though in Mongo it was originally issued in TAXONOMY_1
        // This is because there is a SS record in TAXONOMY_2 that was declustered from rs3
        this.reader = new DeprecatedVariantMongoReader(ASSEMBLY, TAXONOMY_2, mongoClient, TEST_DB, CHUNK_SIZE,
                                                       new DbsnpCollectionNames());
        this.reader.open(new ExecutionContext());
        deprecatedVariants = this.readIntoList();
        assertEquals(2, deprecatedVariants.size());
        assertTrue(Arrays.asList("rs2", "rs3").contains(deprecatedVariants.get(0).getMainId()));
        assertTrue(Arrays.asList("rs2", "rs3").contains(deprecatedVariants.get(1).getMainId()));
    }

    private DbsnpSubmittedVariantEntity createSS(String assembly, int taxonomy, Long ssAccession, Long rsAccession,
                                                 Long start, String reference, String alternate) {
        return new DbsnpSubmittedVariantEntity(ssAccession, "hash" + ssAccession, assembly, taxonomy,
                                               "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false,
                                               false, false, 1);
    }

    private DbsnpClusteredVariantEntity createRS(SubmittedVariantEntity sve, Integer alternateTaxonomy) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        int taxonomyToUse = Objects.isNull(alternateTaxonomy)? sve.getTaxonomyAccession(): alternateTaxonomy;
        ClusteredVariant cv = new ClusteredVariant(sve.getReferenceSequenceAccession(), taxonomyToUse,
                                                   sve.getContig(),
                                                   sve.getStart(),
                                                   new Variant(sve.getContig(), sve.getStart(), sve.getStart(),
                                                               sve.getReferenceAllele(),
                                                               sve.getAlternateAllele()).getType(),
                                                   true, null);
        String hash = hashingFunction.apply(cv);
        return new DbsnpClusteredVariantEntity(sve.getClusteredVariantAccession(), hash, cv);
    }

}
