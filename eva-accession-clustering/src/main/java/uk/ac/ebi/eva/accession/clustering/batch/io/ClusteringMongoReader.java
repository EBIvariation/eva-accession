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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;


public class ClusteringMongoReader implements ItemStreamReader<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ClusteringMongoReader.class);

    private static final String ASSEMBLY_FIELD = "seq";

    private static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    private static final String ID_FIELD = "_id";

    private String assembly;

    private MongoCursor<Document> evaCursor;

    private MongoCursor<Document> dbsnpCursor;

    private MongoConverter converter;

    private MongoTemplate mongoTemplate;

    private int chunkSize;

    //decides whether already clustered or non clustered variants will be read by mongo reader
    private boolean readOnlyClusteredVariants;

    public ClusteringMongoReader(MongoTemplate mongoTemplate, String assembly, int chunkSize,
                                 boolean readOnlyClusteredVariants) {
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.chunkSize = chunkSize;
        this.readOnlyClusteredVariants = readOnlyClusteredVariants;
    }

    @Override
    public SubmittedVariantEntity read() {
        // Read from dbsnp collection first and subsequently EVA collection
        Document nextElement = (dbsnpCursor != null && dbsnpCursor.hasNext()) ? dbsnpCursor.next() :
                (evaCursor.hasNext() ? evaCursor.next() : null);
        return (nextElement != null) ? getSubmittedVariantEntity(nextElement) : null;
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document notClusteredSubmittedVariants) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(notClusteredSubmittedVariants));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        Bson query = Filters.and(Filters.in(ASSEMBLY_FIELD, assembly),
                                 Filters.exists(CLUSTERED_VARIANT_ACCESSION_FIELD, readOnlyClusteredVariants));
        logger.info("Issuing find: {}", query);

        if (readOnlyClusteredVariants){
            FindIterable<Document> submittedVariantsDbsnp =
                    getSubmittedVariants(query, DbsnpSubmittedVariantEntity.class);
            dbsnpCursor = submittedVariantsDbsnp.iterator();
        } else {
            // When clustering variant that do not have any RSID we do not want to retrieve any dbSNP submitted variants
            // We set the cursor for this purpose
            dbsnpCursor = null;
        }
        FindIterable<Document> submittedVariantsEVA =
                getSubmittedVariants(query, SubmittedVariantEntity.class);
        evaCursor = submittedVariantsEVA.iterator();

        converter = mongoTemplate.getConverter();
    }

    private FindIterable<Document> getSubmittedVariants(Bson query, Class<?> entityClass) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(entityClass))
                            .find(query)
                            .sort(Sorts.ascending(ID_FIELD))
                            .noCursorTimeout(true)
                            .batchSize(chunkSize);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        if (dbsnpCursor != null) {
            dbsnpCursor.close();
        }
        evaCursor.close();
    }
}
