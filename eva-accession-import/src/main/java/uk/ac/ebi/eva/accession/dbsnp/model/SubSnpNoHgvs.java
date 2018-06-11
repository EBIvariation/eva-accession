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

import java.sql.Date;

public class SubSnpNoHgvs {

    private String alleles;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private String chromosome;

    private int chromosomeStart;

    private String contigName;

    private Orientation subsnpOrientation;

    private Orientation snpOrientation;

    private Orientation contigOrientation;

    private int contigStart;

    private boolean frequencyExists;

    private boolean genotypeExists;

    private String reference;

    private Date createTime;

    private int taxonomyId;

    // TODO: sort the fields in a logical order, or just leave them in alphabetical order?
    public SubSnpNoHgvs(String alleles, String assembly, String batchHandle, String batchName,
                        String chromosome, int chromosomeStart, String contigName,
                        Orientation subsnpOrientation,
                        Orientation snpOrientation, Orientation contigOrientation,
                        int contigStart, boolean frequencyExists,
                        boolean genotypeExists, String reference, Date createTime,
                        int taxonomyId) {
        this.alleles = alleles;
        this.assembly = assembly;
        this.batchHandle = batchHandle;
        this.batchName = batchName;
        this.chromosome = chromosome;
        this.chromosomeStart = chromosomeStart;
        this.contigName = contigName;
        this.subsnpOrientation = subsnpOrientation;
        this.snpOrientation = snpOrientation;
        // TODO: check if we need other orientation fields to calculate the variant alleles
        this.contigOrientation = contigOrientation;
        this.contigStart = contigStart;
        this.frequencyExists = frequencyExists;
        this.genotypeExists = genotypeExists;
        this.reference = reference;
        this.createTime = createTime;
        this.taxonomyId = taxonomyId;
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

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getChromosomeStart() {
        return chromosomeStart;
    }

    public void setChromosomeStart(int chromosomeStart) {
        this.chromosomeStart = chromosomeStart;
    }

    public String getContigName() {
        return contigName;
    }

    public void setContigName(String contigName) {
        this.contigName = contigName;
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
}
