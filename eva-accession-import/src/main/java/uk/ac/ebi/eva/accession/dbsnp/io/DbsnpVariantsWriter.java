/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import com.mongodb.DuplicateKeyException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private MongoTemplate mongoTemplate;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;


    public DbsnpVariantsWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate);
        this.dbsnpSubmittedVariantOperationWriter = new DbsnpSubmittedVariantOperationWriter(mongoTemplate);
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
            dbsnpSubmittedVariantWriter.write(submittedVariants);

            List<DbsnpSubmittedVariantOperationEntity> operations = dbsnpVariantsWrapper.getOperations();
            if (operations != null && !operations.isEmpty()) {
                dbsnpSubmittedVariantOperationWriter.write(operations);
            }
        }
        writeClusteredVariants(wrappers);
    }

    private void writeClusteredVariants(List<? extends DbsnpVariantsWrapper> items) {
        try {
            Collection<DbsnpClusteredVariantEntity> uniqueClusteredVariants =
                    items.stream()
                         .map(DbsnpVariantsWrapper::getClusteredVariant)
                         .collect(Collectors.toMap(DbsnpClusteredVariantEntity::getHashedMessage,
                                                   a -> a,
                                                   (a, b) -> a))
                         .values();
            dbsnpClusteredVariantWriter.write(new ArrayList<>(uniqueClusteredVariants));
        } catch (DuplicateKeyException e) {
            /*
                We don't group by accession because several documents can have the same one. This will have to be
                cleaned up later via merge, deprecation or other means. But even though we group by hash so that a
                ClusteredVariant is only written once, this is only guaranteed within a chunk. It's possible that a
                ClusteredVariant is split across chunks. Also, performing inserts and ignoring the exception seems a
                bit simpler than performing upserts, as that would require retries if executing concurrently.
                See https://jira.mongodb.org/browse/SERVER-14322
             */
         }
    }

}
