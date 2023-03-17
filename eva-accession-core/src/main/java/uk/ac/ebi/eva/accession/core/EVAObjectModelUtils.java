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
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import java.util.Objects;
import java.util.function.Function;

/***
 * Utilities to obtain/transform objects in the EVA model
 */
public class EVAObjectModelUtils {
    public static String getClusteredVariantHash(ISubmittedVariant submittedVariant) {
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariant);
        Function<IClusteredVariant, String>  clusteredHashingFunction = (new ClusteredVariantSummaryFunction())
                .andThen(new SHA1HashingFunction());
        return clusteredHashingFunction.apply(clusteredVariant);
    }

    public static ClusteredVariant toClusteredVariant(ISubmittedVariant submittedVariant) {
        ClusteredVariant cv = new ClusteredVariant(submittedVariant.getReferenceSequenceAccession(),
                submittedVariant.getTaxonomyAccession(), submittedVariant.getContig(),
                submittedVariant.getStart(),
                VariantClassifier.getVariantClassification(submittedVariant.getReferenceAllele(),
                        submittedVariant.getAlternateAllele()), submittedVariant.isValidated(),
                submittedVariant.getCreatedDate());
        if (Objects.nonNull(submittedVariant.getMapWeight())) cv.setMapWeight(submittedVariant.getMapWeight());
        return cv;
    }

    public static ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(),
                getClusteredVariantHash(submittedVariantEntity), toClusteredVariant(submittedVariantEntity));
    }

    public static ClusteredVariantEntity toClusteredVariantEntity(Long clusteredVariantAccession,
                                                           IClusteredVariant clusteredVariant) {
        Function<IClusteredVariant, String> clusteredHashingFunction = (new ClusteredVariantSummaryFunction())
                .andThen(new SHA1HashingFunction());
        return new ClusteredVariantEntity(clusteredVariantAccession, clusteredHashingFunction.apply(clusteredVariant),
                clusteredVariant);
    }

    public static SubmittedVariantEntity toSubmittedVariantEntity(Long submittedVariantAccession,
                                                           ISubmittedVariant submittedVariant) {
        Function<ISubmittedVariant, String> submittedHashingFunction = (new SubmittedVariantSummaryFunction())
                .andThen(new SHA1HashingFunction());
        return new SubmittedVariantEntity(submittedVariantAccession, submittedHashingFunction.apply(submittedVariant),
                submittedVariant, 1);
    }
}
