/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SubmittedVariantDeclusterProcessor implements ItemProcessor<DbsnpVariantsWrapper, DbsnpVariantsWrapper> {

    static final String DECLUSTERED = "Declustered: ";

    static final String DECLUSTERED_ALLELES_MISMATCH =
            "None of the variant alleles match the reference allele.";

    static final String DECLUSTERED_TYPE_MISMATCH =
            "The variant type inferred from the alleles does not match the one asserted by dbSNP.";

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubmittedVariantDeclusterProcessor() {
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());

    }

    @Override
    public DbsnpVariantsWrapper process(DbsnpVariantsWrapper dbsnpVariantsWrapper) throws Exception {
        List<DbsnpSubmittedVariantEntity> submittedVariants = new ArrayList<>();
        List<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();

        for (DbsnpSubmittedVariantEntity submittedVariantEntity: dbsnpVariantsWrapper.getSubmittedVariants()) {
            List<String> reasons = new ArrayList<>();
            if (variantNeedsDecluster(dbsnpVariantsWrapper, submittedVariantEntity, reasons)) {
                submittedVariantEntity = decluster(submittedVariantEntity, operations, reasons);
            }
            submittedVariants.add(submittedVariantEntity);
        }
        dbsnpVariantsWrapper.setSubmittedVariants(submittedVariants);
        dbsnpVariantsWrapper.setOperations(operations);
        return dbsnpVariantsWrapper;
    }

    private boolean variantNeedsDecluster(DbsnpVariantsWrapper wrapper, ISubmittedVariant submittedVariant,
                                          List<String> reasons) {
        if (!submittedVariant.isAllelesMatch()) {
            reasons.add(DECLUSTERED_ALLELES_MISMATCH);
        }
        boolean typeMatches = isSameType(wrapper.getClusteredVariant(), submittedVariant,
                                         wrapper.getDbsnpVariantType());
        if (!typeMatches) {
            reasons.add(DECLUSTERED_TYPE_MISMATCH);
        }
        return !reasons.isEmpty();
    }

    private boolean isSameType(DbsnpClusteredVariantEntity clusteredVariant, ISubmittedVariant submittedVariant,
                               DbsnpVariantType dbsnpVariantType) {
        try {
            return VariantClassifier.getVariantClassification(submittedVariant.getReferenceAllele(),
                                                              submittedVariant.getAlternateAllele(),
                                                              dbsnpVariantType.intValue())
                                    .equals(clusteredVariant.getType());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public DbsnpSubmittedVariantEntity decluster(DbsnpSubmittedVariantEntity variantEntityToDecluster,
                                                 List<DbsnpSubmittedVariantOperationEntity> operations,
                                                 List<String> reasons) {
        DbsnpSubmittedVariantOperationEntity operation = createOperation(variantEntityToDecluster, reasons);
        operations.add(operation);

        DbsnpSubmittedVariantEntity declusteredVariantEntity =
                new DbsnpSubmittedVariantEntity(variantEntityToDecluster.getAccession(),
                                                variantEntityToDecluster.getHashedMessage(),
                                                variantEntityToDecluster.getModel(),
                                                variantEntityToDecluster.getVersion());

        declusteredVariantEntity.setClusteredVariantAccession(null);
        return declusteredVariantEntity;
    }

    public DbsnpSubmittedVariantOperationEntity createOperation(DbsnpSubmittedVariantEntity variantEntityToDecluster,
                                                                List<String> reasons) {
        DbsnpSubmittedVariantOperationEntity operation = new DbsnpSubmittedVariantOperationEntity();

        Long accession = variantEntityToDecluster.getAccession();
        String reason = DECLUSTERED + String.join(" ", reasons);
        DbsnpSubmittedVariantInactiveEntity inactiveEntity = new DbsnpSubmittedVariantInactiveEntity(
                variantEntityToDecluster);

        operation.fill(EventType.UPDATED, accession, null, reason, Collections.singletonList(inactiveEntity));
        return operation;
    }

}
