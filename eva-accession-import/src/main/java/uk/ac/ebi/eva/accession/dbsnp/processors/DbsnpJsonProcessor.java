/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpJson;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;

/**
 * This class transforms a given DbsnpJson object into an IVariant type
 */
public class DbsnpJsonProcessor implements ItemProcessor<DbsnpJson, IVariant> {

    public DbsnpJsonProcessor() {
    }

    /**
     * Process the dbsnpJson object to produce an iVariant type
     * @param dbsnpJson dbsnp reader model
     * @return iVariant dbsnpVariant object
     */
    @Override
    public IVariant process(DbsnpJson dbsnpJson) {
        long end = dbsnpJson.getStart() + Math.max(
            dbsnpJson.getReferenceAllele().length(), dbsnpJson.getAlternateAllele().length());
        return new DbsnpVariant(
            dbsnpJson.getContig(),
            dbsnpJson.getStart(),
            end,
            dbsnpJson.getReferenceAllele(),
            dbsnpJson.getAlternateAllele()
        );
    }
}