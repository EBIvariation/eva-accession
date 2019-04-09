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

package uk.ac.ebi.eva.accession.release.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpClusteredVariantOperationEntity.json"
})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class ContigMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY_ACCESSION = "GCF_000409795.2";

    @Autowired
    private MongoClient mongoClient;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Test
    public void basicActiveContigsRead() {
        ContigMongoReader reader = ContigMongoReader.activeContigReader(ASSEMBLY_ACCESSION, mongoClient, TEST_DB);
        reader.open(new ExecutionContext());
        String contig;
        List<String> contigs = new ArrayList<>();
        while ((contig = reader.read()) != null) {
            contigs.add(contig);
        }
        assertEquals(new HashSet<>(Arrays.asList("CM001954.1", "CM001941.2")), new HashSet<>(contigs));
    }

    @Test
    public void basicMergedContigsRead() {
        ContigMongoReader reader = ContigMongoReader.mergedContigReader(ASSEMBLY_ACCESSION, mongoClient, TEST_DB);
        reader.open(new ExecutionContext());
        String contig;
        List<String> contigs = new ArrayList<>();
        while ((contig = reader.read()) != null) {
            contigs.add(contig);
        }
        assertEquals(new HashSet<>(Arrays.asList("CM001954.1", "CM001941.2", "CM000346.1", "CM000347.1")), new HashSet<>(contigs));
    }
}
