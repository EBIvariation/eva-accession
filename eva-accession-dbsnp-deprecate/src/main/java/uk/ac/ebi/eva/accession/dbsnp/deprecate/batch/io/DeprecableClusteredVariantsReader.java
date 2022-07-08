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
package uk.ac.ebi.eva.accession.dbsnp.deprecate.batch.io;

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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read all ClusteredVariants in the dbsnpClusteredVariantEntityDeclustered collection that are not in the main
 * collection dbsnpClusteredVariantEntity
 *
 * DeprecableClusteredVariantsReader = dbsnpClusteredVariantEntityDeclustered - dbsnpClusteredVariantEntity
 */
public class DeprecableClusteredVariantsReader implements ItemStreamReader<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DeprecableClusteredVariantsReader.class);

    private static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED = "dbsnpClusteredVariantEntityDeclustered";

    private static final String ACCESSION_FIELD = "accession";

    private static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    private static final String ACTIVE = "active";

    private static final String ASSEMBLY_FIELD = "asm";

    private MongoClient mongoClient;

    private String database;

    private List<String> assemblies;

    private MongoCursor<Document> cursor;

    private MongoTemplate mongoTemplate;
    
    private MongoConverter converter;

    private int chunkSize;

    /**
     * Constructs a reader for all variants in the collection DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED, irrespective of the assembly.
     */
    public DeprecableClusteredVariantsReader(MongoClient mongoClient, String database, MongoTemplate mongoTemplate,
                                             int chunkSize) {
        this(mongoClient, database, mongoTemplate, null, chunkSize);
    }

    /**
     * Constructs a reader that retrieves variants mapped only against the specified assemblies.
     */
    public DeprecableClusteredVariantsReader(MongoClient mongoClient, String database, MongoTemplate mongoTemplate,
                                             List<String> assemblyAccessions, int chunkSize) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.mongoTemplate = mongoTemplate;
        this.assemblies = assemblyAccessions;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED);
        AggregateIterable<Document> declusteredVariants = collection.aggregate(buildAggregation())
                                                                    .allowDiskUse(true)
                                                                    .useCursor(true)
                                                                    .batchSize(chunkSize);
        cursor = declusteredVariants.iterator();
        converter = mongoTemplate.getConverter();
    }

    private List<Bson> buildAggregation() {
        List<Bson> aggregation = new ArrayList<>();
        if (assemblies != null && !assemblies.isEmpty()) {
            aggregation.add(Aggregates.match(Filters.in(ASSEMBLY_FIELD, assemblies)));
        }
        aggregation.add(
                Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, ACCESSION_FIELD, CLUSTERED_VARIANT_ACCESSION_FIELD,
                                  ACTIVE));
        aggregation.add(Aggregates.match(Filters.eq(ACTIVE, Collections.EMPTY_LIST)));

        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    @Override
    public DbsnpClusteredVariantEntity read() {
        return cursor.hasNext() ? getDbsnpClusteredVariantEntity(cursor.next()) : null;
    }

    private DbsnpClusteredVariantEntity getDbsnpClusteredVariantEntity(Document deprecableClusteredVariant) {
        return converter.read(DbsnpClusteredVariantEntity.class, new BasicDBObject(deprecableClusteredVariant));
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }
}
