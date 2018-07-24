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
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import java.util.Collections;
import java.util.List;

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
    public void write(List<? extends DbsnpVariantsWrapper> items) throws Exception {
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : items) {
            List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
            dbsnpSubmittedVariantWriter.write(submittedVariants);
            dbsnpClusteredVariantWriter.write(Collections.singletonList(dbsnpVariantsWrapper.getClusteredVariant()));
            declusterAllelesMismatch(submittedVariants);
        }

    }

    private void declusterAllelesMismatch(List<DbsnpSubmittedVariantEntity> submittedVariants)
            throws AccessionDeprecatedException, AccessionDoesNotExistException, AccessionMergedException,
                   HashAlreadyExistsException {
        for (int i = 0; i < submittedVariants.size(); i++) {
            DbsnpSubmittedVariantEntity submittedVariant = submittedVariants.get(i);
            if (!submittedVariant.isAllelesMatch()) {
                SubmittedVariant model = submittedVariant.getModel();
                model.setClusteredVariantAccession(null);
                service.update(submittedVariant.getAccession(), i+1, model); // i+1: versions start with 1
            }
        }
    }

}
