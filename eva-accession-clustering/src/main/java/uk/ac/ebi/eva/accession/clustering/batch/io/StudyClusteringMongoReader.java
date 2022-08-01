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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
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

import java.util.List;

public class StudyClusteringMongoReader implements ItemStreamReader<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(StudyClusteringMongoReader.class);

    static final String ASSEMBLY_FIELD = "seq";

    static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    static final String STUDY_FIELD = "study";

    String assembly;

    List<String> studies;

    MongoCursor<Document> evaCursor;

    MongoConverter converter;

    MongoTemplate mongoTemplate;

    int chunkSize;

    public StudyClusteringMongoReader(MongoTemplate mongoTemplate, String assembly, List<String> studies,
                                      int chunkSize) {
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.studies = studies;
        this.chunkSize = chunkSize;
    }

    @Override
    public SubmittedVariantEntity read() {
        Document nextElement = evaCursor.tryNext();
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
        evaCursor = initializeCursor();
        converter = mongoTemplate.getConverter();
    }

    private MongoCursor<Document> initializeCursor() {
        Bson query = Filters.and(Filters.in(ASSEMBLY_FIELD, assembly),
                                 Filters.in(STUDY_FIELD, studies),
                                 Filters.exists(CLUSTERED_VARIANT_ACCESSION_FIELD, false),
                                 Filters.exists(SubmittedVariantEntity.backPropagatedRSAttribute, false));
        logger.info("Issuing find: {}", query);

        FindIterable<Document> submittedVariants = getSubmittedVariants(query);
        return submittedVariants.iterator();
    }

    private FindIterable<Document> getSubmittedVariants(Bson query) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(SubmittedVariantEntity.class))
                            .find(query)
                            .noCursorTimeout(true)
                            .batchSize(chunkSize);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        if (evaCursor != null) {
            evaCursor.close();
        }
    }
}
