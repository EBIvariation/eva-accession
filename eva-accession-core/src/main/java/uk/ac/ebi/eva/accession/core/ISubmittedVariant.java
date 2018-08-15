/*
 *
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
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;

import java.time.LocalDateTime;

/**
 * Abstract representation of the fields that uniquely identify an accessioned submitted variant.
 *
 * Implemented by the basic bean {@link SubmittedVariant}, the entity for EVA accessions
 * {@link SubmittedVariantEntity}, and the entities for DbSNP accessions (Subsnps with and without coordinates).
 */
public interface ISubmittedVariant {

    boolean DEFAULT_SUPPORTED_BY_EVIDENCE = true;

    boolean DEFAULT_ASSEMBLY_MATCH = true;

    boolean DEFAULT_ALLELES_MATCH = true;

    boolean DEFAULT_VALIDATED = false;

    String getAssemblyAccession();

    int getTaxonomyAccession();

    String getProjectAccession();

    String getContig();

    long getStart();

    String getReferenceAllele();

    String getAlternateAllele();

    Long getClusteredVariantAccession();

    /**
     * @return True if this submitted variant is supported by genotypes or frequencies. Nullable (= true) when
     * serialized to database in order to reduce disk usage.
     */
    Boolean isSupportedByEvidence();

    /**
     * @return True if the reference allele matches the reference genome. Nullable (= true) when serialized to
     * database in order to reduce disk usage.
     */
    Boolean isAssemblyMatch();

    /**
     * @return False if reference allele was not found in the alleles list. Nullable (= true) when serialized to
     * database in order to reduce disk usage.
     */
    Boolean isAllelesMatch();

    /**
     * @return True if the variant was curated manually, and not only detected by computational methods.
     * see https://www.ncbi.nlm.nih.gov/books/NBK21088/table/ch5.ch5_t4/?report=objectonly . Nullable (= false) when
     * serialized to database in order to reduce disk usage.
     */
    Boolean isValidated();

    LocalDateTime getCreatedDate();

}
