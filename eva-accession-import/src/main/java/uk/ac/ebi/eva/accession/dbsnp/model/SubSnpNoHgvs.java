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
import org.apache.commons.lang3.math.NumberUtils;

import uk.ac.ebi.eva.commons.core.models.Region;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubSnpNoHgvs {

    public static final String STR_SEQUENCE_REGEX_GROUP = "sequence";

    private Long ssId;

    private Long rsId;

    private String alleles;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private String chromosome;

    private Long chromosomeStart;

    private String contigName;

    private long contigStart;

    private DbsnpVariantType dbsnpVariantType;

    private Orientation subsnpOrientation;

    private Orientation snpOrientation;

    private Orientation contigOrientation;

    private boolean subsnpValidated;

    private boolean snpValidated;

    private boolean frequencyExists;

    private boolean genotypeExists;

    private String reference;

    private Timestamp ssCreateTime;

    private Timestamp rsCreateTime;

    private int taxonomyId;

    private boolean assemblyMatch;

    /**
     * This pattern will detect STRs like (A)5 or (TA)7, being the sequence group the not numeric part
     */
    private static Pattern microsatellitePattern = Pattern.compile("(?<" + STR_SEQUENCE_REGEX_GROUP + ">\\([ATCG]+\\))\\d+");


    public SubSnpNoHgvs(Long ssId, Long rsId, String reference, String alleles, String assembly, String batchHandle,
                        String batchName, String chromosome, Long chromosomeStart, String contigName, long contigStart,
                        DbsnpVariantType dbsnpVariantType, Orientation subsnpOrientation, Orientation snpOrientation,
                        Orientation contigOrientation, boolean subsnpValidated, boolean snpValidated,
                        boolean frequencyExists, boolean genotypeExists, Timestamp ssCreateTime, Timestamp rsCreateTime,
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
        this.dbsnpVariantType = dbsnpVariantType;
        this.subsnpOrientation = subsnpOrientation;
        this.snpOrientation = snpOrientation;
        this.contigOrientation = contigOrientation;
        this.subsnpValidated = subsnpValidated;
        this.snpValidated = snpValidated;
        this.frequencyExists = frequencyExists;
        this.genotypeExists = genotypeExists;
        this.reference = reference;
        this.ssCreateTime = ssCreateTime;
        this.rsCreateTime = rsCreateTime;
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

    public DbsnpVariantType getDbsnpVariantType() {
        return dbsnpVariantType;
    }

    public void setDbsnpVariantType(DbsnpVariantType dbsnpVariantType) {
        this.dbsnpVariantType = dbsnpVariantType;
    }

    public Orientation getSubsnpOrientation() {
        return subsnpOrientation;
    }

    public void setSubsnpOrientation(Orientation subsnpOrientation) {
        this.subsnpOrientation = subsnpOrientation;
    }

    public Orientation getSnpOrientation() {
        return snpOrientation;
    }

    public void setSnpOrientation(Orientation snpOrientation) {
        this.snpOrientation = snpOrientation;
    }

    public Orientation getContigOrientation() {
        return contigOrientation;
    }

    public void setContigOrientation(Orientation contigOrientation) {
        this.contigOrientation = contigOrientation;
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

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
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
        if (contigOrientation.equals(Orientation.REVERSE)) {
            return getTrimmedAllele(calculateReverseComplement(reference));
        } else {
            return getTrimmedAllele(reference);
        }
    }

    /**
     * Removes leading and trailing spaces. Replaces a dash allele with an empty string.
     */
    private static String getTrimmedAllele(String allele) {
        if (allele == null) {
            return "";
        }
        allele = allele.trim();
        if (allele.equals("-")) {
            return "";
        }
        return allele;
    }

    public List<String> getAlternateAllelesInForwardStrand() {
        List<String> altAllelesInForwardStrand = new ArrayList<>();

        List<String> alleles = getAllelesInForwardStrand();
        String reference = getReferenceInForwardStrand();

        for (String allele : alleles) {
            if (!allele.equals(reference)) {
                if (dbsnpVariantType.equals(DbsnpVariantType.MICROSATELLITE)) {
                    altAllelesInForwardStrand.add(getMicrosatelliteAlternate(allele, alleles.get(0)));
                } else {
                    altAllelesInForwardStrand.add(allele);
                }
            }
        }

        return altAllelesInForwardStrand;
    }

    /**
     * This method splits the alleles and convert them to the forward strand if necessary. This method will return all
     * alleles, including the reference one
     * @return Array containing each allele in the forward strand
     */
    private List<String> getAllelesInForwardStrand() {
        Orientation allelesOrientation;
        try {
            allelesOrientation = getAllelesOrientation();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown alleles orientation for variant " + this, e);
        }

        // We use StringUtils because String.split does not remove the leading empty fields after splitting
        Stream<String> trimmedAlleles = Arrays.stream(StringUtils.split(alleles, "/"))
                                              .map(SubSnpNoHgvs::getTrimmedAllele);

        if (allelesOrientation.equals(Orientation.FORWARD)) {
            return trimmedAlleles.collect(Collectors.toList());
        } else if (allelesOrientation.equals(Orientation.REVERSE)) {
            return trimmedAlleles.map(SubSnpNoHgvs::calculateReverseComplement)
                                 .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException(
                    "Unknown alleles orientation " + allelesOrientation + " for variant " + this);
        }
    }

    private Orientation getAllelesOrientation() {
        return Orientation.getOrientation(
                this.subsnpOrientation.getValue() * this.snpOrientation.getValue() * this.contigOrientation.getValue());
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

    private String getMicrosatelliteAlternate(String alternate, String firstAltAllele) {
        if (NumberUtils.isDigits(alternate)) {
            Matcher matcher = microsatellitePattern.matcher(firstAltAllele);
            if (matcher.matches()) {
                alternate = matcher.group(STR_SEQUENCE_REGEX_GROUP) + alternate;
            } else {
                throw new IllegalArgumentException("Not parseable STR: " + reference + "/" + alternate);
            }
        }

        return alternate;
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
