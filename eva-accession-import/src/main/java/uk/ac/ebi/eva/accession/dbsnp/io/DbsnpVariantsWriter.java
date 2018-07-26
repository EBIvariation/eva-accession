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
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private MongoTemplate mongoTemplate;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private SubmittedVariantAccessioningService service;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate, SubmittedVariantAccessioningService service) {
        this.mongoTemplate = mongoTemplate;
        this.service = service;
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate);
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
            dbsnpSubmittedVariantWriter.write(submittedVariants);
            declusterAllelesMismatch(submittedVariants);
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
            ; // even though we grouped by hash (not by accession: there can be several different documents with the
            // same accession, those will have to be deprecated later) so that a ClusteredVariant is only written
            // once, this is only within a chunk. It's possible that a ClusteredVariant is split across chunks. Also,
            // doing inserts and ignore the exception seems a bit simpler than doing upserts, as that would require
            // doing retries if we work concurrently:  https://jira.mongodb.org/browse/SERVER-14322
        }
    }

    private void declusterAllelesMismatch(List<DbsnpSubmittedVariantEntity> submittedVariants)
            throws AccessionDeprecatedException, AccessionDoesNotExistException, AccessionMergedException,
                   HashAlreadyExistsException {
        for (DbsnpSubmittedVariantEntity submittedVariant : submittedVariants) {
            if (!submittedVariant.isAllelesMatch()) {
                SubmittedVariant model = submittedVariant.getModel();
                model.setClusteredVariantAccession(null);
                service.update(submittedVariant.getAccession(), submittedVariant.getVersion(), model);
            }
        }
    }

}
