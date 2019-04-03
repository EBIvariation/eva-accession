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

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import java.util.Arrays;
import java.util.List;

public class ContigMongoReader implements ItemStreamReader<String> {

    private static final Logger logger = LoggerFactory.getLogger(ContigMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String ACTIVE_REFERENCE_ASSEMBLY_FIELD = "asm";

    private static final String DEPRECATED_REFERENCE_ASSEMBLY_FIELD = "inactiveObjects.asm";

    private static final String ACTIVE_CONTIG_KEY = "$contig";

    private static final String DEPRECATED_CONTIG_KEY = "$inactiveObjects.contig";

    private static final String MONGO_ID_FIELD = "_id";

    private static final String GET_ELEMENT_MONGO_OPERATOR = "$arrayElemAt";

    protected String assemblyAccession;

    protected MongoClient mongoClient;

    protected String database;

    protected MongoCursor<Document> cursor;

    private final String collection;

    private final List<Bson> aggregation;

    public static ContigMongoReader activeContigReader(String assemblyAccession, MongoClient mongoClient,
                                                       String database) {
        return new ContigMongoReader(assemblyAccession, mongoClient, database,
                                     DBSNP_CLUSTERED_VARIANT_ENTITY,
                                     buildAggregationForActiveContigs(assemblyAccession));
    }

    public static ContigMongoReader deprecatedContigReader(String assemblyAccession, MongoClient mongoClient,
                                                           String database) {
        return new ContigMongoReader(assemblyAccession, mongoClient, database,
                                     DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY,
                                     buildAggregationForDeprecatedContigs(assemblyAccession));
    }

    private ContigMongoReader(String assemblyAccession, MongoClient mongoClient, String database, String collection,
                              List<Bson> aggregation) {
        this.assemblyAccession = assemblyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
        this.collection = collection;
        this.aggregation = aggregation;
    }

    private static List<Bson> buildAggregationForActiveContigs(String assemblyAccession) {
        Bson match = Aggregates.match(Filters.eq(ACTIVE_REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson uniqueContigs = Aggregates.group(ACTIVE_CONTIG_KEY);
        List<Bson> aggregation = Arrays.asList(match, uniqueContigs);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    private static List<Bson> buildAggregationForDeprecatedContigs(String assemblyAccession) {
        Bson match = Aggregates.match(Filters.and(Filters.eq(DEPRECATED_REFERENCE_ASSEMBLY_FIELD, assemblyAccession),
                                                  Filters.eq("eventType", EventType.DEPRECATED.toString())));

        Bson extractContig = Aggregates.project(new Document(MONGO_ID_FIELD, DEPRECATED_CONTIG_KEY));

        Document getFirstContig = new Document(GET_ELEMENT_MONGO_OPERATOR, Arrays.asList("$" + MONGO_ID_FIELD, 0));
        Bson projectArrayToSingleContig = Aggregates.project(new Document(MONGO_ID_FIELD, getFirstContig));

        Bson uniqueContigs = Aggregates.group(MONGO_ID_FIELD);

        List<Bson> aggregation = Arrays.asList(match, extractContig, projectArrayToSingleContig, uniqueContigs);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(collection);
    }

    private void aggregate(String collectionName) {
        logger.debug("Preparing query to database {}, collection {}", database, collectionName);
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(aggregation)
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true);
        cursor = clusteredVariants.iterator();
    }

    @Override
    public String read() throws UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getContig(cursor.next()) : null;
    }

    private String getContig(Document clusteredVariant) {
        return clusteredVariant.getString(MONGO_ID_FIELD);
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }
}
