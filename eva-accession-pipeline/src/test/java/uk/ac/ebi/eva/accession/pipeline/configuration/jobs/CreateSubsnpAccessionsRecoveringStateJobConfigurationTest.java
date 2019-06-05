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

package uk.ac.ebi.eva.accession.pipeline.configuration.jobs;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.service.SubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.pipeline.test.MongoTestConfiguration;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@UsingDataSet(locations = {
        "/test-data/submittedVariantEntity.json"})
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        MongoTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-recover-test.properties")
public class CreateSubsnpAccessionsRecoveringStateJobConfigurationTest {

    private static final long UNCOMMITTED_ACCESSION = 5000000000L;

    private static final String TEST_DB = "test-db";

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockService blockService;


    @Autowired
    private ContiguousIdBlockRepository blockRepository;

    @Autowired
    private SubmittedVariantAccessioningDatabaseService databaseService;

    @Autowired
    private InputParameters inputParameters;

    //needed for @UsingDataSet
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Value("${accessioning.submitted.categoryId}")
    private String categoryId;

    @Value("${accessioning.instanceId}")
    private String applicationInstanceId;


    @After
    public void tearDown() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getFasta() + ".fai"));
    }

    @Test
    public void accessionGeneratorShouldRecoverUncommittedAccessions() throws Exception {
        startWithAnUncommittedAccessionInMongo();

        SubmittedVariantMonotonicAccessioningService service =
                instantiateAnAccessioningServiceThatRecoversTheUncommittedAccession();

        List<Long> generatedAccessions = accessionANewObject(service);

        assertThatTheUncommittedAccessionWasNotReused(generatedAccessions);
    }

    private void startWithAnUncommittedAccessionInMongo() {
        assertEquals(1, repository.count());
        assertEquals(1, repository.findByAccession(UNCOMMITTED_ACCESSION).size());
        assertEquals(1, blockRepository.count());

        // This means that the last committed accession is the previous to the UNCOMMITTED_ACCESSION
        assertEquals(UNCOMMITTED_ACCESSION - 1, blockRepository.findAll().iterator().next().getLastCommitted());
    }

    private SubmittedVariantMonotonicAccessioningService instantiateAnAccessioningServiceThatRecoversTheUncommittedAccession() {
        MonotonicAccessionGenerator<ISubmittedVariant> accessionGenerator = new MonotonicAccessionGenerator<>(
                categoryId, applicationInstanceId, blockService, databaseService);

        return new SubmittedVariantMonotonicAccessioningService(
                accessionGenerator, databaseService, new SubmittedVariantSummaryFunction(), new SHA1HashingFunction());
    }

    private List<Long> accessionANewObject(SubmittedVariantMonotonicAccessioningService service)
            throws AccessionCouldNotBeGeneratedException {
        return service.getOrCreate(Collections.singletonList(
                new SubmittedVariant("assembly", 1111, "project", "contig", 100, "A", "T", null, false, false, false,
                                     false, null)))
                      .stream()
                      .map(AccessionWrapper::getAccession)
                      .collect(Collectors.toList());
    }

    private void assertThatTheUncommittedAccessionWasNotReused(List<Long> generatedAccessions) {
        assertEquals(1, generatedAccessions.size());
        Long secondAccessionWrapper = generatedAccessions.get(0);
        assertEquals(1, repository.findByAccession(secondAccessionWrapper).size());

        assertEquals(1, repository.findByAccession(UNCOMMITTED_ACCESSION).size());
        assertEquals((long)secondAccessionWrapper, UNCOMMITTED_ACCESSION + 1);

        assertEquals(1, blockRepository.count());
        assertEquals((long)secondAccessionWrapper,
                     blockRepository.findAll().iterator().next().getLastCommitted());
    }

}
