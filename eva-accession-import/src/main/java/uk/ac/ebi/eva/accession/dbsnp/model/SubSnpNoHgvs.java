/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.model;

import uk.ac.ebi.eva.commons.core.models.Region;

import java.sql.Date;

public class SubSnpNoHgvs {

    private String alleles;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private Region chromosomeRegion;

    private Region contigRegion;

    private Orientation subsnpOrientation;

    private Orientation snpOrientation;

    private Orientation contigOrientation;

    private int contigStart;

    private boolean frequencyExists;

    private boolean genotypeExists;

    private String reference;

    private Date createTime;

    private int taxonomyId;

    private boolean assemblyMatch;

    public SubSnpNoHgvs(String alleles, String assembly, String batchHandle, String batchName, String chromosome,
                        Long chromosomeStart, String contigName, Orientation subsnpOrientation,
                        Orientation snpOrientation, Orientation contigOrientation, Long contigStart,
                        boolean frequencyExists, boolean genotypeExists, String reference, Date createTime,
                        int taxonomyId) {

        this.alleles = alleles;
        this.assembly = assembly;
        this.batchHandle = batchHandle;
        this.batchName = batchName;
        this.chromosomeRegion = createRegion(chromosome, chromosomeStart);
        this.contigRegion = createRegion(contigName, contigStart);
        this.subsnpOrientation = subsnpOrientation;
        this.snpOrientation = snpOrientation;
        this.contigOrientation = contigOrientation;
        this.frequencyExists = frequencyExists;
        this.genotypeExists = genotypeExists;
        this.reference = reference;
        this.createTime = createTime;
        this.taxonomyId = taxonomyId;
    }

    private Region createRegion(String sequenceName, Long start) {
        if (sequenceName != null) {
            if (start != null) {
                return new Region(sequenceName, start);
            }
            return new Region(sequenceName);
        }
        // This should happen only with chromosomes, when a contig-to-chromosome mapping is not available
        return null;
    }

    public String getAlleles() {
        return alleles;
    }

    public void setAlleles(String alleles) {
        this.alleles = alleles;
    }

    public String getAssembly() {
        return assembly;
    }

    public void setAssembly(String assembly) {
        this.assembly = assembly;
    }

    public String getBatchHandle() {
        return batchHandle;
    }

    public void setBatchHandle(String batchHandle) {
        this.batchHandle = batchHandle;
    }

    public String getBatchName() {
        return batchName;
    }

    public void setBatchName(String batchName) {
        this.batchName = batchName;
    }

    public Region getChromosomeRegion() {
        return chromosomeRegion;
    }

    public void setChromosomeRegion(Region chromosomeRegion) {
        this.chromosomeRegion = chromosomeRegion;
    }

    public Region getContigRegion() {
        return contigRegion;
    }

    public void setContigRegion(Region contigRegion) {
        this.contigRegion = contigRegion;
    }

    public Orientation getContigOrientation() {
        return contigOrientation;
    }

    public void setContigOrientation(Orientation contigOrientation) {
        this.contigOrientation = contigOrientation;
    }

    public int getContigStart() {
        return contigStart;
    }

    public void setContigStart(int contigStart) {
        this.contigStart = contigStart;
    }

    public boolean isFrequencyExists() {
        return frequencyExists;
    }

    public void setFrequencyExists(boolean frequencyExists) {
        this.frequencyExists = frequencyExists;
    }

    public boolean isGenotypeExists() {
        return genotypeExists;
    }

    public void setGenotypeExists(boolean genotypeExists) {
        this.genotypeExists = genotypeExists;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public int getTaxonomyId() {
        return taxonomyId;
    }

    public void setTaxonomyId(int taxonomyId) {
        this.taxonomyId = taxonomyId;
    }

    public boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    public void setAssemblyMatch(boolean assemblyMatch) {
        this.assemblyMatch = assemblyMatch;
    }

    public String getAlternate() {
        String[] alleles = getAlleles().split("/");
        for (String allele : alleles) {
            if (!allele.equals(reference)) {
                return allele;
            }
        }
        // TODO: what if there are several alleles?
        // TODO: complement the alleles according to the orientation
        return null;
    }
}
