/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.Collections;

public class ClusteringMongoReaderRetryable extends ClusteringMongoReader {

    private static final Logger logger = LoggerFactory.getLogger(ClusteringMongoReaderRetryable.class);

    private static final String ID_FIELD = "_id";

    // Used for execution context and recovery
    private static final String CURRENT_ID_KEY = "currentId";
    private static final String CURRENT_COLLECTION_KEY = "currentCollection";
    private static final String DBSNP_COLLECTION = "dbsnp";
    private static final String EVA_COLLECTION = "eva";
    private String currentId;
    private String currentCollection;  // dbSNP or EVA

    private RetryTemplate retryTemplate;

    public static final int MAX_RETRIES = 5;

    //decides whether already clustered or non clustered variants will be read by mongo reader
    private boolean readOnlyClusteredVariants;

    public ClusteringMongoReaderRetryable(MongoTemplate mongoTemplate, String assembly, int chunkSize,
                                 boolean readOnlyClusteredVariants) {
        super(mongoTemplate, assembly, chunkSize, readOnlyClusteredVariants);
    }

    @Override
    public SubmittedVariantEntity read() {
        return retryTemplate.execute(retryContext -> doRead());
    }

    public SubmittedVariantEntity doRead() {
        // Read from dbsnp collection first and subsequently EVA collection
        Document nextElement = null;
        if (dbsnpCursor != null && dbsnpCursor.hasNext()) {
            currentCollection = DBSNP_COLLECTION;
            nextElement = dbsnpCursor.next();
        } else if (evaCursor != null && evaCursor.hasNext()) {
            currentCollection = EVA_COLLECTION;
            nextElement = evaCursor.next();
        }
        if (nextElement != null) {
            SubmittedVariantEntity submittedVariantEntity = getSubmittedVariantEntity(nextElement);
            currentId = submittedVariantEntity.getId();
            return submittedVariantEntity;
        }
        return null;
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document notClusteredSubmittedVariants) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(notClusteredSubmittedVariants));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey(CURRENT_ID_KEY)) {
            this.currentId = executionContext.getString(CURRENT_ID_KEY);
            this.currentCollection = executionContext.getString(CURRENT_COLLECTION_KEY);
        }
        initializeRetryTemplate();
        initializeReader();
    }

    public void initializeRetryTemplate() {
        retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new ClusteringMongoReaderRetryable.MongoReaderRetryPolicy());
        retryTemplate.setBackOffPolicy(new ExponentialRandomBackOffPolicy());
    }

    public void initializeReader() {
        Bson query = Filters.and(Filters.in(ASSEMBLY_FIELD, assembly),
                                 Filters.exists(CLUSTERED_VARIANT_ACCESSION_FIELD, readOnlyClusteredVariants));
        Bson queryWithCurrentId = null;
        if (currentId != null) {
            queryWithCurrentId = Filters.and(query, Filters.gt(ID_FIELD, currentId));
        }

        if (readOnlyClusteredVariants) {
            FindIterable<Document> submittedVariantsDbsnp;
            if (currentId != null && DBSNP_COLLECTION.equals(currentCollection)) {
                logger.info("Issuing find in dbsnp collection: {}", queryWithCurrentId);
                submittedVariantsDbsnp = getSubmittedVariants(queryWithCurrentId, DbsnpSubmittedVariantEntity.class);
            } else {
                logger.info("Issuing find in dbsnp collection: {}", query);
                submittedVariantsDbsnp = getSubmittedVariants(query, DbsnpSubmittedVariantEntity.class);
            }
            dbsnpCursor = submittedVariantsDbsnp.iterator();
        } else {
            // When clustering variant that do not have any RSID we do not want to retrieve any dbSNP submitted variants
            // We set the cursor for this purpose
            dbsnpCursor = null;
        }
        FindIterable<Document> submittedVariantsEVA;
        if (currentId != null && EVA_COLLECTION.equals(currentCollection)) {
            logger.info("Issuing find in eva collection: {}", queryWithCurrentId);
            submittedVariantsEVA = getSubmittedVariants(queryWithCurrentId, SubmittedVariantEntity.class);
        } else {
            logger.info("Issuing find in eva collection: {}", query);
            submittedVariantsEVA = getSubmittedVariants(query, SubmittedVariantEntity.class);
        }
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
        executionContext.put(CURRENT_COLLECTION_KEY, currentCollection);
        executionContext.put(CURRENT_ID_KEY, currentId);
    }

    @Override
    public void close() throws ItemStreamException {
        if (dbsnpCursor != null) {
            dbsnpCursor.close();
        }
        evaCursor.close();
    }


    /**
     * Retry policy for handling MongoCursorNotFoundException during reads.
     */
    class MongoReaderRetryPolicy extends SimpleRetryPolicy {

        public MongoReaderRetryPolicy() {
            super(MAX_RETRIES, Collections.singletonMap(MongoCursorNotFoundException.class, true));
        }

        @Override
        public void registerThrowable(RetryContext context, Throwable throwable) {
            // retry count is incremented in super.registerThrowable(), so we check it's less than maxAttempts-1
            // to ensure we don't re-initialize the reader unnecessarily
            if (canRetry(context) && context.getRetryCount() < getMaxAttempts()-1
                    && throwable instanceof MongoCursorNotFoundException) {
                logger.warn("Retrying MongoCursorNotFoundException: {}", throwable.getMessage());
                initializeReader();
            }
            super.registerThrowable(context, throwable);
        }
    }
}
