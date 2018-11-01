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
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
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

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


@RunWith(SpringRunner.class)
@TestPropertySource("classpath:accession-release-test.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class AccessionedVariantMongoReaderTest {

    private AccessionedVariantMongoReader reader;

    private ExecutionContext executionContext;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("test-db").build());

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        reader = new AccessionedVariantMongoReader();
    }

    @Test
    public void loadTestData() {
        List<SubmittedVariantEntity> submittedVariants =
                mongoTemplate.findAll(SubmittedVariantEntity.class, "dbsnpSubmittedVariantEntity");

        assertNotEquals(0, submittedVariants.size());
    }

    @Test
    public void loadTestDataMongo() {
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        MongoDatabase db = mongoClient.getDatabase("test-db");
        MongoCollection<Document> collection = db.getCollection("dbsnpClusteredVariantEntity");

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("asm", "GCF_000409795.2")),
                Aggregates.lookup("dbsnpSubmittedVariantEntity", "accession", "rs", "ssInfo"),
                Aggregates.sort(orderBy(ascending("contig", "start")))
        )).allowDiskUse(true).batchSize(10).useCursor(true);

        MongoCursor<Document> cursor = result.iterator();

        List<Variant> variants = new ArrayList<>();
        while (cursor.hasNext()) {
            Document clusteredVariant = cursor.next();

            String contig = (String) clusteredVariant.get("contig");
            long start = (long) clusteredVariant.get("start");
            long rs = (long) clusteredVariant.get("accession");
            String reference = "";
            String alternate = "";
            long end = 0L;
            List<VariantSourceEntry> studies = new ArrayList<>();

            List<Document> submittedVariants = (List) clusteredVariant.get("ssInfo");
            for (Document submitedVariant : submittedVariants) {
                reference = (String) submitedVariant.get("ref");
                alternate = (String) submitedVariant.get("alt");
                end = reader.calculateEnd(reference, alternate, start);
                String study = (String) submitedVariant.get("study");
                studies.add(new VariantSourceEntry(study, study));
            }

            Variant variant = new Variant(contig, start, end, reference, alternate);
            variant.setMainId(Objects.toString(rs));
            variant.addSourceEntries(studies);
            variants.add(variant);
        }
        assertEquals(2, variants.size());
     }

    @Test
    public void readerTest() throws Exception {
        reader.open(executionContext);
        FileOutputStream fileOutputStream = new FileOutputStream("/tmp/rs_release_localhost.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        Variant variant;
        int i = 0;
        while ((variant = reader.read()) != null) {
            i++;
            bufferedWriter.write(variant.toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        reader.close();
        assertEquals(2, i);
    }
}
