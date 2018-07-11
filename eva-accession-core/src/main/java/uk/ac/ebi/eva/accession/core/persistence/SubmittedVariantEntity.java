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
package uk.ac.ebi.eva.accession.core.persistence;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import java.util.Objects;

@Document
public class SubmittedVariantEntity extends AccessionedDocument<Long> implements ISubmittedVariant {

    @Field("asm")
    private String assemblyAccession;

    @Field("tax")
    private int taxonomyAccession;

    @Field("study")
    private String projectAccession;

    private String contig;

    private long start;

    @Field("ref")
    private String referenceAllele;

    @Field("alt")
    private String alternateAllele;

    @Indexed
    @Field("rs")
    private Long clusteredVariantAccession;

    @Field("evidence")
    private Boolean supportedByEvidence;

    @Field("asmMatch")
    private Boolean assemblyMatch;

    @Field("allelesMatch")
    private Boolean allelesMatch;

    @Field("validated")
    private Boolean validated;

    protected SubmittedVariantEntity() {
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model) {
        this(accession, hashedMessage, model.getAssemblyAccession(), model.getTaxonomyAccession(),
             model.getProjectAccession(), model.getContig(), model.getStart(), model.getReferenceAllele(),
             model.getAlternateAllele(), model.getClusteredVariantAccession(), model.isSupportedByEvidence(),
             model.isAssemblyMatch(), model.isAllelesMatch(), model.isValidated(), 1);
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, String assemblyAccession,
                                  int taxonomyAccession, String projectAccession, String contig, long start,
                                  String referenceAllele, String alternateAllele, Long clusteredVariantAccession,
                                  Boolean supportedByEvidence, Boolean assemblyMatch, Boolean allelesMatch,
                                  Boolean validated, int version) {
        super(hashedMessage, accession, version);
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.clusteredVariantAccession = clusteredVariantAccession;

        if (supportedByEvidence == null) {
            throw new IllegalArgumentException(
                    "supportedByEvidence should not be null, as null is used for default values");
        } else {
            this.supportedByEvidence = supportedByEvidence == DEFAULT_SUPPORTED_BY_EVIDENCE ? null :
                    supportedByEvidence;
        }

        if (assemblyMatch == null) {
            throw new IllegalArgumentException("assemblyMatch should not be null, as null is used for default values");
        } else {
            this.assemblyMatch = assemblyMatch == DEFAULT_ASSEMBLY_MATCH ? null : assemblyMatch;
        }

        if (allelesMatch == null) {
            throw new IllegalArgumentException("allelesMatch should not be null, as null is used for default values");
        } else {
            this.allelesMatch = allelesMatch == DEFAULT_ALLELES_MATCH ? null :
                    allelesMatch;
        }

        if (validated == null) {
            throw new IllegalArgumentException("validated should not be null, as null is used for default values");
        } else {
            this.validated = validated == DEFAULT_VALIDATED ? null : validated;
        }
    }

    public ISubmittedVariant getModel() {
        SubmittedVariant variant = new SubmittedVariant(this);
        variant.setSupportedByEvidence(
                supportedByEvidence == null ? DEFAULT_SUPPORTED_BY_EVIDENCE : supportedByEvidence);
        variant.setAssemblyMatch(assemblyMatch == null ? DEFAULT_ASSEMBLY_MATCH : assemblyMatch);
        variant.setAllelesMatch(allelesMatch == null ? DEFAULT_ALLELES_MATCH : allelesMatch);
        variant.setValidated(validated == null ? DEFAULT_VALIDATED : validated);
        variant.setCreatedDate(getCreatedDate());
        return variant;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    @Override
    public String getProjectAccession() {
        return projectAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public String getReferenceAllele() {
        return referenceAllele;
    }

    @Override
    public String getAlternateAllele() {
        return alternateAllele;
    }

    @Override
    public Long getClusteredVariantAccession() {
        return clusteredVariantAccession;
    }

    @Override
    public Boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

}
