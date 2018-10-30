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
package uk.ac.ebi.eva.accession.dbsnp.test;

import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.List;
import java.util.function.Function;

import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

public class TestVariantBuilders {

    public static final int TAXONOMY_1 = 3880;

    public static final int TAXONOMY_2 = 3882;


    public static final String PROJECT_1 = "project_1";

    public static final String PROJECT_2 = "project_2";

    public static final Long CLUSTERED_VARIANT_ACCESSION_1 = 12L;

    public static final Long CLUSTERED_VARIANT_ACCESSION_2 = 13L;

    public static final Long CLUSTERED_VARIANT_ACCESSION_3 = 14L;

    public static final int START_1 = 100;

    public static final int START_2 = 200;

    public static final VariantType VARIANT_TYPE = VariantType.SNV;

    public static Function<ISubmittedVariant, String> hashingFunctionSubmitted =
            new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public static Function<IClusteredVariant, String> hashingFunctionClustered =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public static SubmittedVariant defaultSubmittedVariant() {
        return new SubmittedVariant("assembly", TAXONOMY_1, PROJECT_1, "contig", START_1, "reference", "alternate",
                                    CLUSTERED_VARIANT_ACCESSION_1, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                    DEFAULT_ASSEMBLY_MATCH, DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);
    }

    public static SubmittedVariant buildSubmittedVariant(Long clusteredVariantAccession3) {
        SubmittedVariant submittedVariant3 = defaultSubmittedVariant();
        submittedVariant3.setClusteredVariantAccession(clusteredVariantAccession3);
        return submittedVariant3;
    }

    public static SubmittedVariant buildSubmittedVariant(Long clusteredVariantAccession, String project) {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        submittedVariant.setClusteredVariantAccession(clusteredVariantAccession);
        submittedVariant.setProjectAccession(project);
        return submittedVariant;
    }

    public static SubmittedVariant buildSubmittedVariant(Long clusteredVariantAccession, long start) {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        submittedVariant.setClusteredVariantAccession(clusteredVariantAccession);
        submittedVariant.setStart(start);
        return submittedVariant;
    }

    public static ClusteredVariant defaultClusteredVariant() {
        return new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE, DEFAULT_VALIDATED, null);
    }

    public static ClusteredVariant buildClusteredVariant(int start) {
        ClusteredVariant clusteredVariant = defaultClusteredVariant();
        clusteredVariant.setStart(start);
        return clusteredVariant;
    }

    public static DbsnpVariantsWrapper buildSimpleWrapper(List<DbsnpSubmittedVariantEntity> submittedVariantEntities) {
        ClusteredVariant clusteredVariant = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setSubmittedVariants(submittedVariantEntities);
        wrapper.setClusteredVariant(clusteredVariantEntity);
        return wrapper;
    }

    public static DbsnpSubmittedVariantEntity buildSubmittedVariantEntity(Long submittedVariantAccession,
                                                                          SubmittedVariant submittedVariant) {
        return new DbsnpSubmittedVariantEntity(submittedVariantAccession,
                                               hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
    }

    public static DbsnpClusteredVariantEntity buildClusteredVariantEntity(Long clusteredVariantAccession,
                                                                          ClusteredVariant clusteredVariant) {
        return new DbsnpClusteredVariantEntity(clusteredVariantAccession,
                                               hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
    }
}
