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
package uk.ac.ebi.eva.accession.core.model;

import java.time.LocalDateTime;
import java.util.Objects;

import uk.ac.ebi.eva.commons.core.models.VariantType;

public class ClusteredVariant implements IClusteredVariant {

    private String assemblyAccession;

    private int taxonomyAccession;

    private String contig;

    private long start;

    private VariantType type;

    private Boolean validated;

    private LocalDateTime createdDate;

    ClusteredVariant() {

    }

    public ClusteredVariant(IClusteredVariant variant) {
        this(variant.getAssemblyAccession(), variant.getTaxonomyAccession(), variant.getContig(), variant.getStart(),
             variant.getType(), variant.isValidated(), variant.getCreatedDate());
    }

    public ClusteredVariant(String assemblyAccession, int taxonomyAccession, String contig, long start,
                            VariantType type, Boolean validated, LocalDateTime createdDate) {
        if (Objects.isNull(assemblyAccession)) {
            throw new IllegalArgumentException("Assembly accession is required");
        }
        if (Objects.isNull(contig)) {
            throw new IllegalArgumentException("Contig is required");
        }
        if (Objects.isNull(type)) {
            throw new IllegalArgumentException("Variant type is required");
        }

        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.contig = contig;
        this.start = start;
        this.type = type;
        this.validated = validated;
        this.createdDate = createdDate;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public void setTaxonomyAccession(int taxonomyAccession) {
        this.taxonomyAccession = taxonomyAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    public void setContig(String contig) {
        this.contig = contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @Override
    public VariantType getType() {
        return type;
    }

    public void setType(VariantType type) {
        this.type = type;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

    public void setValidated(Boolean validated) {
        this.validated = validated;
    }

    @Override
    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusteredVariant)) {
            return false;
        }

        ClusteredVariant that = (ClusteredVariant) o;

        if (taxonomyAccession != that.taxonomyAccession) {
            return false;
        }
        if (start != that.start) {
            return false;
        }
        if (!assemblyAccession.equals(that.assemblyAccession)) {
            return false;
        }
        if (!contig.equals(that.contig)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        return validated != null ? validated.equals(that.validated) : that.validated == null;
    }

    @Override
    public int hashCode() {
        int result = assemblyAccession.hashCode();
        result = 31 * result + taxonomyAccession;
        result = 31 * result + contig.hashCode();
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + (validated != null ? validated.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusteredVariant{" +
                "assemblyAccession='" + assemblyAccession + '\'' +
                ", taxonomyAccession=" + taxonomyAccession +
                ", contig='" + contig + '\'' +
                ", start=" + start +
                ", type=" + type +
                ", validated=" + validated +
                ", createdDate=" + createdDate +
                '}';
    }
}
