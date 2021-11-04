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
package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

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
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;

import javax.annotation.Nonnull;


public class RSReader implements ItemStreamReader<ClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RSReader.class);

    private static final String ASSEMBLY_FIELD = "asm";

    private final String assembly;

    private MongoCursor<Document> evaCursor;

    private MongoCursor<Document> dbsnpCursor;

    private MongoConverter converter;

    private final MongoTemplate mongoTemplate;

    private final int chunkSize;

    public RSReader(MongoTemplate mongoTemplate, String assembly, int chunkSize) {
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.chunkSize = chunkSize;
    }

    @Override
    public ClusteredVariantEntity read() {
        // Read from dbsnp collection first and subsequently EVA collection
        Document nextElement = dbsnpCursor.hasNext() ? dbsnpCursor.next() :
                (evaCursor.hasNext() ? evaCursor.next() : null);
        return (nextElement != null) ? getClusteredVariantEntity(nextElement) : null;
    }

    private ClusteredVariantEntity getClusteredVariantEntity(Document clusteredVariants) {
        return converter.read(ClusteredVariantEntity.class, new BasicDBObject(clusteredVariants));
    }

    @Override
    public void open(@Nonnull ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        Bson query = Filters.in(ASSEMBLY_FIELD, assembly);
        logger.info("Issuing find: {}", query);

        FindIterable<Document> clusteredVariantsDbsnp =
                getClusteredVariants(query, DbsnpClusteredVariantEntity.class);
        dbsnpCursor = clusteredVariantsDbsnp.iterator();
        FindIterable<Document> clusteredVariantsEVA =
                getClusteredVariants(query, ClusteredVariantEntity.class);
        evaCursor = clusteredVariantsEVA.iterator();

        converter = mongoTemplate.getConverter();
    }

    private FindIterable<Document> getClusteredVariants(Bson query, Class<?> entityClass) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(entityClass))
                            .find(query)
                            .noCursorTimeout(true)
                            .batchSize(chunkSize);
    }

    @Override
    public void update(@Nonnull ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        dbsnpCursor.close();
        evaCursor.close();
    }
}
