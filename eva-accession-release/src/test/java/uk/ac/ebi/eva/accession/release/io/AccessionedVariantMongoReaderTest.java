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
package uk.ac.ebi.eva.accession.release.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;

import java.util.List;

import static org.junit.Assert.assertNotEquals;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:accession-release-test.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@ContextConfiguration(classes = {MongoConfiguration.class})
public class AccessionedVariantMongoReaderTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void loadTestData() {
        List<SubmittedVariantEntity> submittedVariants =
                mongoTemplate.findAll(SubmittedVariantEntity.class, "dbsnpSubmittedVariantEntity");

        assertNotEquals(0, submittedVariants.size());
    }
}
