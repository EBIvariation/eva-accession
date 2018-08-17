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

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;

    private DbsnpClusteredVariantDeclusteredWriter dbsnpClusteredVariantDeclusteredWriter;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate, ImportCounts importCounts) {
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate, importCounts);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        this.dbsnpSubmittedVariantOperationWriter = new DbsnpSubmittedVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantDeclusteredWriter = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        Set<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered = new HashSet<>();
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
            dbsnpSubmittedVariantWriter.write(submittedVariants);

            List<DbsnpSubmittedVariantOperationEntity> operations = dbsnpVariantsWrapper.getOperations();
            if (operations != null && !operations.isEmpty()) {
                dbsnpSubmittedVariantOperationWriter.write(operations);
                clusteredVariantsDeclustered.add(dbsnpVariantsWrapper.getClusteredVariant());
            }
        }
        writeClusteredVariants(wrappers);
        dbsnpClusteredVariantDeclusteredWriter.write(new ArrayList<>(clusteredVariantsDeclustered));
    }

    private void writeClusteredVariants(List<? extends DbsnpVariantsWrapper> items) {
        Collection<DbsnpClusteredVariantEntity> uniqueClusteredVariants =
                items.stream()
                     .map(DbsnpVariantsWrapper::getClusteredVariant)
                     .collect(Collectors.toMap(DbsnpClusteredVariantEntity::getHashedMessage,
                                               a -> a,
                                               (a, b) -> a))
                     .values();
        dbsnpClusteredVariantWriter.write(new ArrayList<>(uniqueClusteredVariants));
    }

}
