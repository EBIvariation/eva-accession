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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.lang.NonNull;

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

public class SplitOrMergedRSReader implements ItemStreamReader<List<SubmittedVariantEntity>> {

    private static final Logger logger = LoggerFactory.getLogger(SplitOrMergedRSReader.class);

    static final String ASSEMBLY_FIELD = "inactiveObjects.asm";

    static final String EVENT_TYPE_FIELD = "eventType";

    String remappedAssembly;

    String originalAssembly;

    ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    SubmittedVariantAccessioningService submittedVariantAccessioningService;

    MongoCursor<Document> evaCursor;

    MongoCursor<Document> dbsnpCursor;

    MongoConverter converter;

    MongoTemplate mongoTemplate;

    int chunkSize;

    public SplitOrMergedRSReader(MongoTemplate mongoTemplate, String remappedAssembly, String originalAssembly,
                                 ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                                 SubmittedVariantAccessioningService submittedVariantAccessioningService,
                                 int chunkSize) {
        this.mongoTemplate = mongoTemplate;
        this.remappedAssembly = remappedAssembly;
        this.originalAssembly = originalAssembly;
        this.clusteredVariantAccessioningService = clusteredVariantAccessioningService;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.chunkSize = chunkSize;
    }

    @Override
    public List<SubmittedVariantEntity> read() {
        List<ClusteredVariantOperationEntity> clusteredVariantOperations = new ArrayList<>();
        for (int i = 0; i < this.chunkSize; i++) {
            ClusteredVariantOperationEntity op = readCursor();
            if (Objects.nonNull(op)) {
                clusteredVariantOperations.add(op);
            }
            else {
                break;
            }
        }
        List<Long> rsToLookUpInRemappedAssembly =
                clusteredVariantOperations.stream().map(ClusteredVariantOperationEntity::getAccession).collect(
                        Collectors.toList());
        rsToLookUpInRemappedAssembly.addAll(
                clusteredVariantOperations.stream().map(ClusteredVariantOperationEntity::getMergedInto)
                                          .collect(Collectors.toList()));
        rsToLookUpInRemappedAssembly.addAll(
                clusteredVariantOperations.stream().map(ClusteredVariantOperationEntity::getSplitInto)
                                          .collect(Collectors.toList()));
        List<Long> ssToLookUpInOriginalAssembly =
                this.submittedVariantAccessioningService
                        .getByClusteredVariantAccessionIn(rsToLookUpInRemappedAssembly)
                        .stream()
                        .filter(e -> e.getData().getReferenceSequenceAccession().equals(this.remappedAssembly))
                        .map(AccessionWrapper::getAccession)
                        .collect(Collectors.toList());
        List<SubmittedVariantEntity> targetSSRecordsInOriginalAssembly =
                this.submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(this.originalAssembly, ssToLookUpInOriginalAssembly)
                        .stream().map(result -> new SubmittedVariantEntity(result.getAccession(), result.getHash(),
                                                                           result.getData(), result.getVersion()))
                        .collect(Collectors.toList());
        return targetSSRecordsInOriginalAssembly.size() == 0 ? null: targetSSRecordsInOriginalAssembly;
    }

    public ClusteredVariantOperationEntity readCursor() {
        // Read from dbsnp collection first and subsequently EVA collection
        Document nextElement;
        if (dbsnpCursor != null && dbsnpCursor.hasNext()) {
            nextElement = dbsnpCursor.next();
        } else {
            if (evaCursor == null) {
                evaCursor = initializeCursor(ClusteredVariantOperationEntity.class);
            }
            nextElement = evaCursor.tryNext();
        }

        return (nextElement != null) ? getClusteredVariantOperationEntity(nextElement) : null;
    }

    private ClusteredVariantOperationEntity getClusteredVariantOperationEntity(Document clusteredVariantOperation) {
        return converter.read(ClusteredVariantOperationEntity.class, new BasicDBObject(clusteredVariantOperation));
    }

    @Override
    public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        dbsnpCursor = initializeCursor(DbsnpClusteredVariantOperationEntity.class);
        evaCursor = null;
        converter = mongoTemplate.getConverter();
    }

    private MongoCursor<Document> initializeCursor(Class<?> entityClass) {
        Bson query = Filters.and(Filters.eq(ASSEMBLY_FIELD, remappedAssembly),
                                 Filters.in(EVENT_TYPE_FIELD,
                                            Arrays.asList(EventType.RS_SPLIT.toString(), EventType.MERGED.toString())));
        logger.info("Issuing find: {}", query);

        FindIterable<Document> clusteredVariantOperations = getClusteredVariantOperations(query, entityClass);
        return clusteredVariantOperations.iterator();
    }

    private FindIterable<Document> getClusteredVariantOperations(Bson query, Class<?> entityClass) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(entityClass))
                            .find(query)
                            .noCursorTimeout(true)
                            .batchSize(chunkSize);
    }

    @Override
    public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        if (dbsnpCursor != null) {
            dbsnpCursor.close();
        }
        if (evaCursor != null) {
            evaCursor.close();
        }
    }
}
