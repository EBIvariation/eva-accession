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

import com.mongodb.BasicDBObject;
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
import org.springframework.data.mongodb.core.convert.MongoConverter;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads historical variants, that have been merged into a later deprecate one, from a MongoDB database.
 */
public class MergedDeprecatedVariantMongoReader implements ItemStreamReader<DbsnpClusteredVariantOperationEntity> {

    private static final Logger logger = LoggerFactory.getLogger(MergedDeprecatedVariantMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

    private static final String INACTIVE_OBJECTS = "inactiveObjects";

    private static final String MERGE_INTO_FIELD = "mergeInto";

    private static final String EVENT_TYPE_FIELD = "eventType";

    private static final String ASSEMBLY_FIELD = "asm";

    private static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    private static final String SS_INFO_FIELD = "ssInfo";

    private final String assemblyAccession;

    private final MongoClient mongoClient;

    private final String database;

    private MongoCursor<Document> cursor;

    private MongoConverter mongoConverter;

    private int chunkSize;

    public MergedDeprecatedVariantMongoReader(String assemblyAccession, MongoClient mongoClient, String database,
                                              MongoConverter mongoConverter, int chunkSize) {
        this.assemblyAccession = assemblyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
        this.mongoConverter = mongoConverter;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);
    }

    private void aggregate(String collectionName) {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true)
                                                                  .batchSize(chunkSize);
        cursor = clusteredVariants.iterator();
    }

    private List<Bson> buildAggregation() {
        Bson matchAssembly = Aggregates.match(Filters.eq(getInactiveField(ASSEMBLY_FIELD), assemblyAccession));
        Bson matchMerged = Aggregates.match(Filters.eq(EVENT_TYPE_FIELD, EventType.MERGED.toString()));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, MERGE_INTO_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        Bson matchEmpty = Aggregates.match(Filters.eq(SS_INFO_FIELD, Collections.EMPTY_LIST));
        List<Bson> aggregation = Arrays.asList(matchAssembly, matchMerged, lookup, matchEmpty);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    private String getInactiveField(String field) {
        return INACTIVE_OBJECTS + "." + field;
    }

    @Override
    public DbsnpClusteredVariantOperationEntity read()
            throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getEntity(cursor.next()) : null;
    }

    /**
     * Converts to {@link DbsnpClusteredVariantOperationEntity} using MongoConverter.
     *
     * Note how we also use the MongoConverter to convert the internal "inactiveObjects". If we didn't do that, the
     * converter puts in
     * {@link uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument#inactiveObjects} a list
     * of Documents instead of a list of DbsnpClusteredVariantInactiveEntity.
     */
    private DbsnpClusteredVariantOperationEntity getEntity(Document operation) {
        List<Document> objects = (List<Document>)operation.get(INACTIVE_OBJECTS);
        operation.put(INACTIVE_OBJECTS, objects.stream()
                                               .map(BasicDBObject::new)
                                               .map(o -> mongoConverter.read(DbsnpClusteredVariantInactiveEntity.class,
                                                                             o))
                                               .collect(Collectors.toList()));

        return mongoConverter.read(DbsnpClusteredVariantOperationEntity.class, new BasicDBObject(operation));
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {

    }
}
