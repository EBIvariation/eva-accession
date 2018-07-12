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

import java.time.LocalDateTime;

/**
 * Abstract representation of the fields that uniquely identify an accessioned clustered variant.
 *
 * Implemented by the basic bean {@link ClusteredVariant}, the entity for EVA accessions (TODO fill reference here), and
 * the entities for DbSNP accessions (regular imported RSs and RSs without coordinates).
 */
public interface IClusteredVariant {

    String getAssemblyAccession();

    int getTaxonomyAccession();

    String getContig();

    long getStart();

    VariantType getType();

    /**
     * @return True if the variant was curated manually, and not only detected by computational methods.
     * see https://www.ncbi.nlm.nih.gov/books/NBK21088/table/ch5.ch5_t4/?report=objectonly
     */
    Boolean isValidated();

    LocalDateTime getCreatedDate();
}
