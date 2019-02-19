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

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Read all ClusteredVariants in the dbsnpClusteredVariantEntityDeclustered collection that are not in the main
 * collection dbsnpClusteredVariantEntity
 *
 * ClusteredVariantsToDeprecateReader = dbsnpClusteredVariantEntityDeclustered - dbsnpClusteredVariantEntity
 */
public class ClusteredVariantsToDeprecateReader implements ItemStreamReader<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredVariantsToDeprecateReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED = "dbsnpClusteredVariantEntityDeclustered";

    private static final String ACCESSION_FIELD = "accession";

    private static final String RS_FIELD = "rs";

    private static final String ID_FIELD = "_id";

    private static final String REFERENCE_ASSEMBLY_FIELD = "asm";

    private static final String TAXONOMY_FIELD = "tax";

    private static final String CONTIG_FIELD = "contig";

    private static final String START_FIELD = "start";

    private static final String TYPE_FIELD = "type";

    private static final String ACCESSION_KEY = "accession";

    private static final String VERSION_KEY = "version";

    private MongoClient mongoClient;

    private String database;

    private MongoCursor<Document> cursor;

    public ClusteredVariantsToDeprecateReader(MongoClient mongoClient, String database) {
        this.mongoClient = mongoClient;
        this.database = database;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED);
        AggregateIterable<Document> declusteredVariants = collection.aggregate(buildAggregation())
                                                                    .allowDiskUse(true)
                                                                    .useCursor(true);
        cursor = declusteredVariants.iterator();
    }

    private List<Bson> buildAggregation() {
        Bson lookup = Aggregates.lookup(DBSNP_CLUSTERED_VARIANT_ENTITY, ACCESSION_FIELD, ACCESSION_FIELD, RS_FIELD);
        Bson match = Aggregates.match(Filters.eq(RS_FIELD, Collections.EMPTY_LIST));
        List<Bson> aggregation = Arrays.asList(lookup, match);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    @Override
    public DbsnpClusteredVariantEntity read() {
        return cursor.hasNext() ? getClusteredVariantToDeprecate(cursor.next()) : null;
    }

    private DbsnpClusteredVariantEntity getClusteredVariantToDeprecate(Document declusteredVariant) {
        String id = declusteredVariant.getString(ID_FIELD);
        String assembly = declusteredVariant.getString(REFERENCE_ASSEMBLY_FIELD);
        int taxonomy = declusteredVariant.getInteger(TAXONOMY_FIELD);
        String contig = declusteredVariant.getString(CONTIG_FIELD);
        long start = declusteredVariant.getLong(START_FIELD);
        String type = declusteredVariant.getString(TYPE_FIELD);
        long accession = declusteredVariant.getLong(ACCESSION_KEY);
        int version = declusteredVariant.getInteger(VERSION_KEY);
        LocalDateTime createdDate = declusteredVariant.getDate("createdDate")
                                                      .toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

        return new DbsnpClusteredVariantEntity(accession, id, assembly, taxonomy, contig, start,
                                               VariantType.valueOf(type), false, createdDate, version);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }
}
