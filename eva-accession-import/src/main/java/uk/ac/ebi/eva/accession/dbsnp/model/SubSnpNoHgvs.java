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
package uk.ac.ebi.eva.accession.dbsnp.model;

import uk.ac.ebi.eva.commons.core.models.Region;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class SubSnpNoHgvs {

    public static final String STR_SEQUENCE_REGEX_GROUP = "sequence";

    private Long ssId;

    private Long rsId;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private String chromosome;

    private Long chromosomeStart;

    private String contigName;

    private long contigStart;

    private final DbsnpVariantAlleles dbSnpVariantAlleles;

    private DbsnpVariantType dbsnpVariantType;

    private boolean subsnpValidated;

    private boolean snpValidated;

    private boolean frequencyExists;

    private boolean genotypeExists;

    private Timestamp ssCreateTime;

    private Timestamp rsCreateTime;

    private int taxonomyId;

    private boolean assemblyMatch;

    public SubSnpNoHgvs(Long ssId, Long rsId, String reference, String alleles, String assembly, String batchHandle,
                        String batchName, String chromosome, Long chromosomeStart, String contigName, long contigStart,
                        DbsnpVariantType dbsnpVariantType, Orientation subsnpOrientation, Orientation snpOrientation,
                        Orientation contigOrientation, boolean subsnpValidated, boolean snpValidated,
                        boolean frequencyExists, boolean genotypeExists, Timestamp ssCreateTime, Timestamp rsCreateTime,
                        int taxonomyId) {
        this.ssId = ssId;
        this.rsId = rsId;
        this.assembly = assembly;
        this.batchHandle = batchHandle;
        this.batchName = batchName;
        this.chromosome = chromosome;
        this.chromosomeStart = chromosomeStart;
        this.contigName = contigName;
        this.contigStart = contigStart;
        this.dbsnpVariantType = dbsnpVariantType;
        this.subsnpValidated = subsnpValidated;
        this.snpValidated = snpValidated;
        this.frequencyExists = frequencyExists;
        this.genotypeExists = genotypeExists;
        this.ssCreateTime = ssCreateTime;
        this.rsCreateTime = rsCreateTime;
        this.taxonomyId = taxonomyId;
        Orientation allelesOrientation = Orientation.getOrientation(
                composeOrientationAssumingForwardIfUnknown(subsnpOrientation, snpOrientation, contigOrientation));
        this.dbSnpVariantAlleles = new DbsnpVariantAlleles(reference, alleles, contigOrientation, allelesOrientation,
                                                           this.dbsnpVariantType);
    }

    private int composeOrientationAssumingForwardIfUnknown(Orientation subsnpOrientation, Orientation snpOrientation,
                                                           Orientation contigOrientation) {
        int orientation = 1;
        orientation *= subsnpOrientation == Orientation.REVERSE ? -1 : 1;
        orientation *= snpOrientation == Orientation.REVERSE ? -1 : 1;
        orientation *= contigOrientation == Orientation.REVERSE ? -1 : 1;
        return orientation;
    }


    public Long getSsId() {
        return ssId;
    }

    public void setSsId(Long ssId) {
        this.ssId = ssId;
    }

    public Long getRsId() {
        return rsId;
    }

    public void setRsId(Long rsId) {
        this.rsId = rsId;
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

    public Long getChromosomeStart() {
        return chromosomeStart;
    }

    public void setChromosomeStart(Long chromosomeStart) {
        this.chromosomeStart = chromosomeStart;
    }

    public String getContigName() {
        return contigName;
    }

    public void setContigName(String contigName) {
        this.contigName = contigName;
    }

    public DbsnpVariantType getDbsnpVariantType() {
        return dbsnpVariantType;
    }

    public void setDbsnpVariantType(DbsnpVariantType dbsnpVariantType) {
        this.dbsnpVariantType = dbsnpVariantType;
    }

    public long getContigStart() {
        return contigStart;
    }

    public void setContigStart(long contigStart) {
        this.contigStart = contigStart;
    }

    public boolean isSubsnpValidated() {
        return subsnpValidated;
    }

    public void setSubsnpValidated(boolean subsnpValidated) {
        this.subsnpValidated = subsnpValidated;
    }

    public boolean isSnpValidated() {
        return snpValidated;
    }

    public void setSnpValidated(boolean snpValidated) {
        this.snpValidated = snpValidated;
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

    public Timestamp getSsCreateTime() {
        return ssCreateTime;
    }

    public void setSsCreateTime(Timestamp ssCreateTime) {
        this.ssCreateTime = ssCreateTime;
    }

    public Timestamp getRsCreateTime() {
        return rsCreateTime;
    }

    public void setRsCreateTime(Timestamp rsCreateTime) {
        this.rsCreateTime = rsCreateTime;
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

    public String getReferenceInForwardStrand() {
        return dbSnpVariantAlleles.getReferenceInForwardStrand();
    }

    private List<String> getAllelesInForwardStrand() {
        return dbSnpVariantAlleles.getAllelesInForwardStrand();
    }

    public List<String> getAlternateAllelesInForwardStrand() {
        String referenceAllele = getReferenceInForwardStrand();
        List<String> alleles = getAllelesInForwardStrand();

        return alleles.stream().filter(allele -> !allele.equals(referenceAllele)).collect(Collectors.toList());
    }

    public boolean doAllelesMatch() {
        String referenceInForwardStrand = getReferenceInForwardStrand();
        List<String> allAllelesInForwardStrand = getAllelesInForwardStrand();
        return allAllelesInForwardStrand.stream().anyMatch(allele -> allele.equals(referenceInForwardStrand));
    }

    public Region getVariantRegion() {
        if (getChromosome() != null) {
            return new Region(getChromosome(), getChromosomeStart());
        } else {
            return new Region(getContigName(), getContigStart());
        }
    }

}
