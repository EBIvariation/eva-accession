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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

public class ClusteringMongoReader implements ItemStreamReader<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ClusteringMongoReader.class);

    private static final String ASSEMBLY_FIELD = "seq";

    private static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    private static final String SUBMITTED_VARIANT_ENTITY = "submittedVariantEntity";

    private MongoClient mongoClient;

    private String database;

    private String assembly;

    private MongoCursor<Document> cursor;

    private MongoConverter converter;

    private MongoTemplate mongoTemplate;

    private int chunkSize;

    public ClusteringMongoReader(MongoClient mongoClient, String database, MongoTemplate mongoTemplate,
                                 String assembly, int chunkSize) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.chunkSize = chunkSize;
    }

    @Override
    public SubmittedVariantEntity read() {
        return cursor.hasNext() ? getSubmittedVariantEntity(cursor.next()) : null;
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document notClusteredSubmittedVariants) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(notClusteredSubmittedVariants));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(SUBMITTED_VARIANT_ENTITY);

        Bson query = Filters.and(Filters.in(ASSEMBLY_FIELD, assembly));
        logger.info("Issuing find: {}", query);
        FindIterable<Document> notClusteredSubmittedVariants = collection.find(query)
                                                                         .noCursorTimeout(true)
                                                                         .batchSize(chunkSize);
        cursor = notClusteredSubmittedVariants.iterator();
        converter = mongoTemplate.getConverter();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }
}
