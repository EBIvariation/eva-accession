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

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class SubSnpNoHgvs {

    private Long ssId;

    private Long rsId;

    private String alleles;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private String chromosome;

    private Long chromosomeStart;

    private String contigName;

    private Long contigStart;

    private Orientation subsnpOrientation;

    private Orientation snpOrientation;

    private Orientation contigOrientation;

    private boolean frequencyExists;

    private boolean genotypeExists;

    private String reference;

    private Date createTime;

    private int taxonomyId;

    private boolean assemblyMatch;

    public SubSnpNoHgvs(Long ssId, Long rsId, String alleles, String assembly, String batchHandle, String batchName,
                        String chromosome, Long chromosomeStart, String contigName, Orientation subsnpOrientation,
                        Orientation snpOrientation, Orientation contigOrientation, Long contigStart,
                        boolean frequencyExists, boolean genotypeExists, String reference, Date createTime,
                        int taxonomyId) {
        this.ssId = ssId;
        this.rsId = rsId;
        this.alleles = alleles;
        this.assembly = assembly;
        this.batchHandle = batchHandle;
        this.batchName = batchName;
        this.chromosome = chromosome;
        this.chromosomeStart = chromosomeStart;
        this.contigName = contigName;
        this.contigStart = contigStart;
        this.subsnpOrientation = subsnpOrientation;
        this.snpOrientation = snpOrientation;
        this.contigOrientation = contigOrientation;
        this.frequencyExists = frequencyExists;
        this.genotypeExists = genotypeExists;
        this.reference = reference;
        this.createTime = createTime;
        this.taxonomyId = taxonomyId;
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

    public Orientation getContigOrientation() {
        return contigOrientation;
    }

    public void setContigOrientation(Orientation contigOrientation) {
        this.contigOrientation = contigOrientation;
    }

    public Long getContigStart() {
        return contigStart;
    }

    public void setContigStart(Long contigStart) {
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

    public String getReferenceInForwardStrand() {
        if (contigOrientation.equals(Orientation.REVERSE)) {
            return calculateReverseComplement(reference);
        } else {
            return reference;
        }
    }

    public List<String> getAlternateAllelesInForwardStrand() {
        List<String> altAllelesInForwardStrand = new ArrayList<>();

        String[] alleles = getAllelesInForwardStrand();
        String reference = getReferenceInForwardStrand();
        for (String allele : alleles) {
            if (!allele.equals(reference)) {
                altAllelesInForwardStrand.add(allele);
            }
        }

        return altAllelesInForwardStrand;
    }

    /**
     * This method split the alleles and convert them to the forward strand if necessary. This method will return all
     * alleles, including the reference one
     * @return Array containing each allele in the forward strand
     */
    private String[] getAllelesInForwardStrand() {
        Orientation allelesOrientation;
        try {
            allelesOrientation = getAllelesOrientation();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown alleles orientation for variant " + this, e);
        }

        // We use StringUtils instead of String.split because it removes the trailing empty values after splitting
        String[] dividedAlleles = StringUtils.split(alleles, "/");
        
        if (allelesOrientation.equals(Orientation.FORWARD)) {
            return dividedAlleles;
        } else if (allelesOrientation.equals(Orientation.REVERSE)) {
            return getReversedComplementedAlleles(dividedAlleles);
        } else {
            throw new IllegalArgumentException(
                    "Unknown alleles orientation " + allelesOrientation + " for variant " + this);
        }
    }

    private Orientation getAllelesOrientation() {
        return Orientation.getOrientation(
                this.subsnpOrientation.getValue() * this.snpOrientation.getValue() * this.contigOrientation.getValue());
    }

    private String[] getReversedComplementedAlleles(String[] alleles) {
        for (int i=0; i < alleles.length; i++) {
            alleles[i] = calculateReverseComplement(alleles[i]);
        }
        return alleles;
    }

    private static String calculateReverseComplement(String alleleInReverseStrand) {
        StringBuilder alleleInForwardStrand = new StringBuilder(alleleInReverseStrand).reverse();
        for (int i = 0; i < alleleInForwardStrand.length(); i++) {
            switch (alleleInForwardStrand.charAt(i)) {
                // Capitalization holds a special meaning for dbSNP so we need to preserve it.
                // See https://www.ncbi.nlm.nih.gov/books/NBK44414/#_Reports_Lowercase_Small_Sequence_Letteri_
                case 'A':
                    alleleInForwardStrand.setCharAt(i, 'T');
                    break;
                case 'a':
                    alleleInForwardStrand.setCharAt(i, 't');
                    break;
                case 'C':
                    alleleInForwardStrand.setCharAt(i, 'G');
                    break;
                case 'c':
                    alleleInForwardStrand.setCharAt(i, 'g');
                    break;
                case 'G':
                    alleleInForwardStrand.setCharAt(i, 'C');
                    break;
                case 'g':
                    alleleInForwardStrand.setCharAt(i, 'c');
                    break;
                case 'T':
                    alleleInForwardStrand.setCharAt(i, 'A');
                    break;
                case 't':
                    alleleInForwardStrand.setCharAt(i, 'a');
                    break;
            }
        }
        return alleleInForwardStrand.toString();
    }
}
