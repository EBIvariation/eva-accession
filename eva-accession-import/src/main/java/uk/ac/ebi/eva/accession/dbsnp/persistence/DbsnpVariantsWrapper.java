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
 *
 */
package uk.ac.ebi.eva.accession.dbsnp.persistence;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;

import java.util.List;

/**
 * Wrapper for all the objects that could be generated based on a single record from dbSNP:
 *
 * - One or more submitted variants (SS), depending on the number of alternate alleles
 *
 * - Zero or one clustered variant (RS)
 *
 * - Zero or more operations, depending on whether deprecations, declustering and/or merges need to applied
 */
public class DbsnpVariantsWrapper {

    private List<DbsnpSubmittedVariantEntity> submittedVariants;

    private DbsnpClusteredVariantEntity clusteredVariant;

    private List<DbsnpSubmittedVariantOperationEntity> operations;

    DbsnpVariantType dbsnpVariantType;

    public DbsnpVariantsWrapper() {
    }

    public List<DbsnpSubmittedVariantEntity> getSubmittedVariants() {
        return submittedVariants;
    }

    public void setSubmittedVariants(List<DbsnpSubmittedVariantEntity> submittedVariants) {
        this.submittedVariants = submittedVariants;
    }

    public DbsnpClusteredVariantEntity getClusteredVariant() {
        return clusteredVariant;
    }

    public void setClusteredVariant(DbsnpClusteredVariantEntity clusteredVariant) {
        this.clusteredVariant = clusteredVariant;
    }

    public List<DbsnpSubmittedVariantOperationEntity> getOperations() {
        return operations;
    }

    public void setOperations(List<DbsnpSubmittedVariantOperationEntity> operations) {
        this.operations = operations;
    }

    public DbsnpVariantType getDbsnpVariantType() {
        return dbsnpVariantType;
    }

    public void setDbsnpVariantType(DbsnpVariantType dbsnpVariantType) {
        this.dbsnpVariantType = dbsnpVariantType;
    }
}
