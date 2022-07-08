/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.batch.io;

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

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

/**
 * Read all ClusteredVariants in the dbsnpClusteredVariantEntityDeclustered collection that are not in the main
 * collection dbsnpClusteredVariantEntity
 *
 * DeprecableClusteredVariantsReader = dbsnpClusteredVariantEntityDeclustered - dbsnpClusteredVariantEntity
 */
public class StudySubmittedVariantsReader implements ItemStreamReader<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(StudySubmittedVariantsReader.class);

    static final String ASSEMBLY_FIELD = "seq";

    static final String STUDY_FIELD = "study";

    String assembly;

    String study;

    MongoCursor<Document> evaCursor;

    MongoConverter converter;

    MongoTemplate mongoTemplate;

    int chunkSize;

    private static final String ID_FIELD = "_id";

    public StudySubmittedVariantsReader(String assembly, String study, MongoTemplate mongoTemplate, int chunkSize) {
        this.assembly = assembly;
        this.study = study;
        this.mongoTemplate = mongoTemplate;
        this.chunkSize = chunkSize;
    }

    @Override
    public SubmittedVariantEntity read() {
        // Read from dbsnp collection first and subsequently EVA collection
        Document nextElement = null;
        if (evaCursor != null && evaCursor.hasNext()) {
            nextElement = evaCursor.next();
        }
        if (nextElement != null) {
            return getSubmittedVariantEntity(nextElement);
        }
        return null;
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document notClusteredSubmittedVariants) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(notClusteredSubmittedVariants));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        Bson query = Filters.and(Filters.eq(ASSEMBLY_FIELD, assembly), Filters.eq(STUDY_FIELD, this.study));
        FindIterable<Document> submittedVariantsEVA;
        logger.info("Issuing find in EVA collection: {}", query);
        submittedVariantsEVA = getSubmittedVariants(query, SubmittedVariantEntity.class);
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
        evaCursor.close();
    }
}
