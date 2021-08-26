/*
 *
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.List;

@Document
public class ReleaseRecordEntity {

    public static final String SS_INFO_FIELD = "ssInfo";
    @Id
    private final String ID;

    private final long accession;

    private final String hashedMessage;

    @Indexed(background = true)
    @Field("asm")
    private final String assemblyAccession;

    @Field("tax")
    private final int taxonomyAccession;

    private final String contig;

    private final long start;

    private final VariantType type;

    private final Boolean validated;

    private final Integer mapWeight;

    @Field(SS_INFO_FIELD)
    private final List<ReleaseRecordSubmittedVariantEntity> associatedSubmittedVariantEntities;

    public ReleaseRecordEntity(String ID, long accession, String hashedMessage, String assemblyAccession,
                               int taxonomyAccession, String contig, long start,
                               VariantType type, Boolean validated, Integer mapWeight,
                               List<ReleaseRecordSubmittedVariantEntity> associatedSubmittedVariantEntities) {
        this.ID = ID;
        this.accession = accession;
        this.hashedMessage = hashedMessage;
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.contig = contig;
        this.start = start;
        this.type = type;
        this.validated = validated;
        this.mapWeight = mapWeight;
        this.associatedSubmittedVariantEntities = associatedSubmittedVariantEntities;
    }

    public String getID() {
        return ID;
    }

    public long getAccession() {
        return accession;
    }

    public String getHashedMessage() {
        return hashedMessage;
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public String getContig() {
        return contig;
    }

    public long getStart() {
        return start;
    }

    public VariantType getType() {
        return type;
    }

    public Boolean getValidated() {
        return validated;
    }

    public Integer getMapWeight() {
        return mapWeight;
    }

    public List<ReleaseRecordSubmittedVariantEntity> getAssociatedSubmittedVariantEntities() {
        return associatedSubmittedVariantEntities;
    }
}
