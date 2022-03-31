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
package uk.ac.ebi.eva.accession.core.service.nonhuman;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasNaming;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:rs-accession-test.properties")
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class ClusteredVariantAccessioningServiceTest {

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("clustered-variants-test").build());

    @Autowired
    private ClusteredVariantAccessioningService service;

    private Function<IClusteredVariant, String> clusteredHashingFunction =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    @After
    public void tearDown() throws Exception {
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void sameAccessionsAreReturnedForIdenticalVariants() throws Exception {
        List<ClusteredVariant> variants = Arrays.asList(
                new ClusteredVariant("assembly", 1111, "contig_1", 100, VariantType.SNV, false, null),
                new ClusteredVariant("assembly", 1111, "contig_1", 200, VariantType.SNV, false, null));

        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> generatedAccessions =
                service.getOrCreate(variants);
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> retrievedAccessions =
                service.getOrCreate(variants);

        assertEquals(new HashSet<>(generatedAccessions), new HashSet<>(retrievedAccessions));
    }

    @Test
    public void mergeRemappedVariants() throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        long rs1 = 10L;
        long rs2 = 11L;
        ClusteredVariant cv1 = new ClusteredVariant("asm1", 1000, "1", 100, VariantType.SNV, false, null);
        mongoTemplate.insert(new DbsnpClusteredVariantEntity(rs1, clusteredHashingFunction.apply(cv1), cv1));
        ClusteredVariant cv2 = new ClusteredVariant("asm2", 1000, "1", 100, VariantType.SNV, false, null);
        mongoTemplate.insert(new DbsnpClusteredVariantEntity(rs2, clusteredHashingFunction.apply(cv2), cv2));

        assertEquals(2, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantOperationEntity.class));

        // when
        service.merge(rs2, rs1, "remapping detected these variants are actually the same, but in different assemblies");

        // then
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantOperationEntity.class));
    }

    @UsingDataSet(locations = {"/test-data/dbsnpClusteredVariantEntity.json"})
    @Test
    public void getAllByAccession() throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        assertEquals(2, service.getAllByAccession(1314L, ContigAliasNaming.INSDC).size());
    }
}
