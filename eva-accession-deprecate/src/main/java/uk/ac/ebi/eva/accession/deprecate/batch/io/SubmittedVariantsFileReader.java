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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read all SubmittedVariants for a given assembly whose ids are given in the input file
 */
public class SubmittedVariantsFileReader implements ItemStreamReader<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantsFileReader.class);

    private static final String ASSEMBLY_FIELD = "seq";
    private static final String ACCESSION_FIELD = "accession";

    private BufferedReader reader;
    private boolean endOfFile = false;
    private String assembly;
    private String variantIdFile;
    private MongoCursor<Document> evaCursor;
    private MongoConverter converter;
    private MongoTemplate mongoTemplate;
    private int chunkSize;

    public SubmittedVariantsFileReader(String assembly, String variantIdFile, MongoTemplate mongoTemplate, int chunkSize) {
        this.assembly = assembly;
        this.variantIdFile = variantIdFile;
        this.mongoTemplate = mongoTemplate;
        this.chunkSize = chunkSize;
    }

    @Override
    public SubmittedVariantEntity read() {
        if (evaCursor == null || !evaCursor.hasNext()) {
            if (endOfFile) {
                return null;
            }

            loadNextBatchAndQuery();

            if (evaCursor == null || !evaCursor.hasNext()) {
                return null;
            }
        }

        Document nextElement = evaCursor.next();
        return getSubmittedVariantEntity(nextElement);
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document document) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(document));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            reader = new BufferedReader(new FileReader(variantIdFile));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + variantIdFile + ") with variant IDs", e);
        }
        initializeReader();
    }

    public void initializeReader() {
        converter = mongoTemplate.getConverter();
        loadNextBatchAndQuery();
    }

    private void loadNextBatchAndQuery() {
        List<Long> variantIds = new ArrayList<>();
        String line;

        try {
            while (variantIds.size() < chunkSize && (line = reader.readLine()) != null) {
                variantIds.add(Long.parseLong(line.trim()));
            }
            if (variantIds.isEmpty()) {
                endOfFile = true;
                return;
            }
        } catch (IOException e) {
            throw new ItemStreamException("Error reading variant IDs from file", e);
        }

        Bson query = Filters.and(Filters.in(ACCESSION_FIELD, variantIds), Filters.eq(ASSEMBLY_FIELD, assembly));
        logger.info("Issuing find in EVA collection for a batch of IDs: {}", query);
        FindIterable<Document> submittedVariantsEVA = getSubmittedVariants(query, SubmittedVariantEntity.class);
        evaCursor = submittedVariantsEVA.iterator();
    }

    private FindIterable<Document> getSubmittedVariants(Bson query, Class<?> entityClass) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(entityClass))
                .find(query)
                .noCursorTimeout(true)
                .batchSize(chunkSize);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (evaCursor != null) {
                evaCursor.close();
            }
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Exception while closing resources", e);
        }
    }
}
