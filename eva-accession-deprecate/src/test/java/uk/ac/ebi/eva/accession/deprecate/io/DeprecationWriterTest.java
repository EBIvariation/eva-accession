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
package uk.ac.ebi.eva.accession.deprecate.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntityDeclustered.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/dbsnpClusteredVariantEntity.json"
})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DeprecationWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED = "dbsnpClusteredVariantEntityDeclustered";

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private DeprecationWriter writer;

    private ExecutionContext executionContext;

    private Function<IClusteredVariant, String> function;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        function = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        writer = new DeprecationWriter(mongoTemplate);
    }

    @Test
    public void write() throws Exception {
        // given
        List<DbsnpClusteredVariantEntity> deprecableClusteredVariants = getDeprecableClusteredVariants();
        Set<Long> accessions = deprecableClusteredVariants.stream()
                                                          .map(AccessionedDocument::getAccession)
                                                          .collect(Collectors.toSet());

        long declusteredBefore = mongoTemplate.count(query(where("accession").in(accessions)),
                                                     DbsnpClusteredVariantEntity.class,
                                                     DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED);
        long clusteredBefore = mongoTemplate.count(query(where("accession").in(accessions)),
                                                   DbsnpClusteredVariantEntity.class);
        long operationsBefore = mongoTemplate.count(query(where("accession").in(accessions)),
                                                    DbsnpClusteredVariantOperationEntity.class);
        assertNotEquals(0, declusteredBefore);
        assertNotEquals(0, clusteredBefore);
        assertEquals(0, operationsBefore);

        // when
        writer.write(deprecableClusteredVariants);

        // then
        long declusteredAfter = mongoTemplate.count(query(where("accession").in(accessions)),
                                                    DbsnpClusteredVariantEntity.class,
                                                    DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED);
        long clusteredAfter = mongoTemplate.count(query(where("accession").in(accessions)),
                                                  DbsnpClusteredVariantEntity.class);
        long operationsAfter = mongoTemplate.count(query(where("accession").in(accessions)),
                                                   DbsnpClusteredVariantOperationEntity.class);
        assertEquals(0, declusteredAfter);
        assertEquals(0, clusteredAfter);
        assertNotEquals(0, operationsAfter);
        assertOperations(deprecableClusteredVariants);
    }

    private List<DbsnpClusteredVariantEntity> getDeprecableClusteredVariants() {
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = newClusteredVariantEntity(853357167L, "GCA_000331145.1",
                                                                                        3827, "CM001767.1", 45273241,
                                                                                        VariantType.SNV, false);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = newClusteredVariantEntity(853461378L, "GCA_000331145.1",
                                                                                        3827, "CM001768.1", 31898089,
                                                                                        VariantType.SNV, false);
        return Arrays.asList(clusteredVariantEntity1,
                             clusteredVariantEntity2);
    }

    private DbsnpClusteredVariantEntity newClusteredVariantEntity(Long accession, String assembly, int taxonomy,
                                                                  String contig, long start, VariantType type,
                                                                  Boolean validated) {
        ClusteredVariant variant = new ClusteredVariant(assembly, taxonomy, contig, start, type, validated,
                                                        LocalDateTime.now());
        return new DbsnpClusteredVariantEntity(accession, function.apply(variant), variant);
    }

    private void assertOperations(List<DbsnpClusteredVariantEntity> clusteredVariants) {
        assertEquals(clusteredVariants.size(),
                     mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY).count());

        for (DbsnpClusteredVariantEntity clusteredVariant : clusteredVariants) {
            assertTrue(mongoTemplate.find(new Query(), DbsnpClusteredVariantOperationEntity.class).stream()
                                    .filter( c -> c.getEventType().equals(EventType.DEPRECATED))
                                    .anyMatch(c -> (c.getInactiveObjects().get(0).getAccession()
                                                    .equals(clusteredVariant.getAccession()))));
        }
    }
}