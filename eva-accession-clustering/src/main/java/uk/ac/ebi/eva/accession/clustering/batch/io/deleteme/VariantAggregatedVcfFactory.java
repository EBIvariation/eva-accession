/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 * Copyright 2015 OpenCB
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
package uk.ac.ebi.eva.accession.clustering.batch.io.deleteme;

import uk.ac.ebi.eva.commons.core.models.VariantStatistics;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.genotype.Genotype;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link VariantVcfFactory} that handles aggregated VCF files which
 * have no samples data (e.g. genotypes).
 */
public class VariantAggregatedVcfFactory extends VariantVcfFactory {

    private final Pattern singleNuc = Pattern.compile("^[ACTG]$");

    private final Pattern singleRef = Pattern.compile("^R$");

    private final Pattern refAlt = Pattern.compile("^([ACTG])([ACTG])$");

    private final Pattern refRef = Pattern.compile("^R{2}$");

    private final Pattern altNum = Pattern.compile("^A(\\d+)$");

    private final Pattern altNumaltNum = Pattern.compile("^A(\\d+)A(\\d+)$");

    private final Pattern altNumRef = Pattern.compile("^A(\\d+)R$");

    private final Pattern numNum = Pattern.compile("^(\\d+)[|/](\\d+)$");

    protected Properties tagMap;

    protected Map<String, String> reverseTagMap;

    public static final String GENOTYPE_COUNT = "GTC";

    public static final String GENOTYPE_STRING = "GTS";

    public VariantAggregatedVcfFactory() {
        this(null);
    }

    /**
     * @param mappings Properties that contains case-sensitive tag mapping for aggregation data. A valid example
     *                 structure of this file is:
     *                 <pre>
     *                               {@code
     *
     *                               EUR.AF=EUR_AF
     *                               EUR.AC=AC_EUR
     *                               EUR.AN=EUR_AN
     *                               EUR.GTC=EUR_GTC
     *                               ALL.AF=AF
     *                               ALL.AC=TAC
     *                               ALL.AN=AN
     *                               ALL.GTC=GTC
     *                               }
     *                               </pre>
     *                 <p>
     *                 <p>
     *                 where the right side of the '=' is how the values appear in the vcf, and left side is how it will
     *                 loaded. It must be a bijection, i.e. there must not be repeated entries in any side. The part
     *                 before the '.' can be any string naming the group. The part after the '.' must be one of AF,
     *                 AC, AN or GTC.
     */
    public VariantAggregatedVcfFactory(Properties mappings) {
        this.tagMap = mappings;
        if (this.tagMap != null) {
            this.reverseTagMap = new LinkedHashMap<>(tagMap.size());
            for (String tag : tagMap.stringPropertyNames()) {
                this.reverseTagMap.put(tagMap.getProperty(tag), tag);
            }
        } else {
            this.reverseTagMap = null;
        }
    }

    @Override
    protected void parseSplitSampleData(VariantSourceEntry variantSourceEntry, String[] fields,
                                        int alternateAlleleIdx) {
        if (fields.length > 8) {
            throw new IllegalArgumentException("Aggregated VCFs should not have column FORMAT nor " +
                    "further sample columns, i.e. there should be only 8 columns");
        }
    }

    @Override
    protected void setOtherFields(Variant variant, String fileId, String studyId, Set<String> ids, float quality,
                                  String filter, String info, int numAllele, String[] alternateAlleles, String line) {
        super.setOtherFields(variant, fileId, studyId, ids, quality, filter, info, numAllele, alternateAlleles, line);

        VariantSourceEntry variantSourceEntry = variant.getSourceEntry(fileId, studyId);
        if (tagMap == null) {
            parseStats(variant, variantSourceEntry, numAllele, alternateAlleles, info);
        } else {
            parseCohortStats(variant, variantSourceEntry, numAllele, alternateAlleles, info);
        }
    }

    protected void parseStats(Variant variant, VariantSourceEntry sourceEntry, int numAllele, String[] alternateAlleles,
                              String info) {
        VariantStatistics vs = new VariantStatistics(variant);
        Map<String, String> stats = new LinkedHashMap<>();
        String[] splittedInfo = info.split(";");
        for (String attribute : splittedInfo) {
            String[] assignment = attribute.split("=");

            if (assignment.length == 2 && (assignment[0].equals(ALLELE_COUNT) || assignment[0].equals(ALLELE_NUMBER)
                    || assignment[0].equals(ALLELE_FREQUENCY) || assignment[0].equals(GENOTYPE_COUNT)
                    || assignment[0].equals(GENOTYPE_STRING))) {
                stats.put(assignment[0], assignment[1]);
            }
        }

        addStats(variant, sourceEntry, numAllele, alternateAlleles, stats, vs);

        sourceEntry.setStats(vs);
    }

    protected void parseCohortStats(Variant variant, VariantSourceEntry sourceEntry, int numAllele,
                                    String[] alternateAlleles, String info) {
        Map<String, Map<String, String>> cohortStats = new LinkedHashMap<>();
        // cohortName -> (statsName -> statsValue): EUR->(AC->3,2)
        String[] splittedInfo = info.split(";");
        for (String attribute : splittedInfo) {
            String[] assignment = attribute.split("=");

            if (assignment.length == 2 && reverseTagMap.containsKey(assignment[0])) {
                String opencgaTag = reverseTagMap.get(assignment[0]);
                String[] tagSplit = opencgaTag.split("\\.");
                String cohortName = tagSplit[0];
                String statName = tagSplit[1];
                Map<String, String> parsedValues = cohortStats.get(cohortName);
                if (parsedValues == null) {
                    parsedValues = new LinkedHashMap<>();
                    cohortStats.put(cohortName, parsedValues);
                }
                parsedValues.put(statName, assignment[1]);
            }
        }

        for (String cohortName : cohortStats.keySet()) {
            VariantStatistics vs = new VariantStatistics(variant);
            addStats(variant, sourceEntry, numAllele, alternateAlleles, cohortStats.get(cohortName), vs);
            sourceEntry.setCohortStats(cohortName, vs);
        }
    }

    /**
     * sets (if the map of attributes contains AF, AC, AF and GTC) alleleCount, refAlleleCount, maf, mafAllele,
     * alleleFreq and genotypeCounts,
     *
     * @param variant
     * @param sourceEntry
     * @param numAllele
     * @param alternateAlleles
     * @param attributes
     * @param variantStats
     */
    private void addStats(Variant variant, VariantSourceEntry sourceEntry, int numAllele, String[] alternateAlleles,
                          Map<String, String> attributes, VariantStatistics variantStats) {

        if (attributes.containsKey(ALLELE_NUMBER) && attributes.containsKey(ALLELE_COUNT)) {
            int total = Integer.parseInt(attributes.get(ALLELE_NUMBER));
            String[] alleleCountString = attributes.get(ALLELE_COUNT).split(",");

            if (alleleCountString.length != alternateAlleles.length) {
                return;
            }

            int[] alleleCount = new int[alleleCountString.length];

            String mafAllele = variant.getReference();
            int referenceCount = total;

            for (int i = 0; i < alleleCountString.length; i++) {
                alleleCount[i] = Integer.parseInt(alleleCountString[i]);
                if (i == numAllele) {
                    variantStats.setAltAlleleCount(alleleCount[i]);
                }
                referenceCount -= alleleCount[i];
            }

            variantStats.setRefAlleleCount(referenceCount);
            float maf = (float) referenceCount / total;

            for (int i = 0; i < alleleCount.length; i++) {
                float auxMaf = (float) alleleCount[i] / total;
                if (auxMaf < maf) {
                    maf = auxMaf;
                    mafAllele = alternateAlleles[i];
                }
            }

            variantStats.setMaf(maf);
            variantStats.setMafAllele(mafAllele);
        }

        if (attributes.containsKey(ALLELE_FREQUENCY)) {
            String[] afs = attributes.get(ALLELE_FREQUENCY).split(",");
            if (afs.length == alternateAlleles.length) {
                variantStats.setAltAlleleFreq(Float.parseFloat(afs[numAllele]));
                if (variantStats.getMaf() == -1) {  // in case that we receive AFs but no ACs
                    float sumFreq = 0;
                    for (String af : afs) {
                        sumFreq += Float.parseFloat(af);
                    }
                    float maf = 1 - sumFreq;
                    String mafAllele = variantStats.getRefAllele();

                    for (int i = 0; i < afs.length; i++) {
                        float auxMaf = Float.parseFloat(afs[i]);
                        if (auxMaf < maf) {
                            maf = auxMaf;
                            mafAllele = alternateAlleles[i];
                        }
                    }
                    variantStats.setMaf(maf);
                    variantStats.setMafAllele(mafAllele);
                }
            }
        }
        if (attributes.containsKey(GENOTYPE_COUNT)) {
            String[] gtcs = attributes.get(GENOTYPE_COUNT).split(",");
            if (sourceEntry.hasAttribute(GENOTYPE_STRING)) {    // GTS contains the format like: GTS=GG,GT,TT or GTS=A1A1,A1R,RR
                addGenotypeWithGTS(variant, sourceEntry, gtcs, alternateAlleles, numAllele, variantStats);
            } else {
                for (int i = 0; i < gtcs.length; i++) {
                    String[] gtcSplit = gtcs[i].split(":");
                    Integer alleles[] = new Integer[2];
                    Integer gtc = 0;
                    String gt = null;
                    boolean parseable = true;
                    if (gtcSplit.length == 1) { // GTC=0,5,8
                        getGenotype(i, alleles);
                        gtc = Integer.parseInt(gtcs[i]);
                        gt = mapToMultiallelicIndex(alleles[0], numAllele) + "/" + mapToMultiallelicIndex(alleles[1],
                                numAllele);
                    } else {    // GTC=0/0:0,0/1:5,1/1:8
                        Matcher matcher = numNum.matcher(gtcSplit[0]);
                        if (matcher.matches()) {    // number/number:number
                            alleles[0] = Integer.parseInt(matcher.group(1));
                            alleles[1] = Integer.parseInt(matcher.group(2));
                            gtc = Integer.parseInt(gtcSplit[1]);
                            gt = mapToMultiallelicIndex(alleles[0], numAllele) + "/" + mapToMultiallelicIndex(
                                    alleles[1], numAllele);
                        } else {
                            if (gtcSplit[0].equals("./.")) {    // ./.:number
                                alleles[0] = -1;
                                alleles[1] = -1;
                                gtc = Integer.parseInt(gtcSplit[1]);
                                gt = "./.";
                            } else {
                                parseable = false;
                            }
                        }
                    }
                    if (parseable) {
                        Genotype genotype = new Genotype(gt, variant.getReference(), alternateAlleles[numAllele]);
                        variantStats.addGenotype(genotype, gtc);
                    }
                }
            }
        }

    }

    /**
     * returns in alleles[] the genotype specified in index in the sequence:
     * 0/0, 0/1, 1/1, 0/2, 1/2, 2/2, 0/3...
     *
     * @param index   in this sequence, starting in 0
     * @param alleles returned genotype.
     */
    public static void getGenotype(int index, Integer alleles[]) {
        int cursor = 0;
        final int MAX_ALLOWED_ALLELES = 100;   // should we allow more than 100 alleles?
        for (int i = 0; i < MAX_ALLOWED_ALLELES; i++) {
            for (int j = 0; j <= i; j++) {
                if (cursor == index) {
                    alleles[0] = j;
                    alleles[1] = i;
                    return;
                }
                cursor++;
            }
        }
    }

    private Genotype parseGenotype(String gt, Variant variant, int numAllele, String[] alternateAlleles) {
        Genotype g;
        Matcher m;

        m = singleNuc.matcher(gt);

        if (m.matches()) { // A,C,T,G
            g = new Genotype(gt + "/" + gt, variant.getReference(), variant.getAlternate());
            return g;
        }
        m = singleRef.matcher(gt);
        if (m.matches()) { // R
            g = new Genotype(variant.getReference() + "/" + variant.getReference(), variant.getReference(),
                             variant.getAlternate());
            return g;
        }

        m = refAlt.matcher(gt);
        if (m.matches()) { // AA,AC,TT,GT,...
            String ref = m.group(1);
            String alt = m.group(2);

            int allele1 = (Arrays.asList(alternateAlleles).indexOf(ref) + 1);
            int allele2 = (Arrays.asList(alternateAlleles).indexOf(alt) + 1);

            int val1 = mapToMultiallelicIndex(allele1, numAllele);
            int val2 = mapToMultiallelicIndex(allele2, numAllele);

            return new Genotype(val1 + "/" + val2, variant.getReference(), variant.getAlternate());
        }

        m = refRef.matcher(gt);
        if (m.matches()) { // RR
            g = new Genotype(variant.getReference() + "/" + variant.getReference(), variant.getReference(),
                             variant.getAlternate());
            return g;
        }

        m = altNum.matcher(gt);
        if (m.matches()) { // A1,A2,A3
            int val = Integer.parseInt(m.group(1));
            val = mapToMultiallelicIndex(val, numAllele);
            return new Genotype(val + "/" + val, variant.getReference(), variant.getAlternate());
        }

        m = altNumaltNum.matcher(gt);
        if (m.matches()) { // A1A2,A1A3...
            int val1 = Integer.parseInt(m.group(1));
            int val2 = Integer.parseInt(m.group(2));
            val1 = mapToMultiallelicIndex(val1, numAllele);
            val2 = mapToMultiallelicIndex(val2, numAllele);
            return new Genotype(val1 + "/" + val2, variant.getReference(), variant.getAlternate());
        }

        m = altNumRef.matcher(gt);
        if (m.matches()) { // A1R, A2R
            int val1 = Integer.parseInt(m.group(1));
            val1 = mapToMultiallelicIndex(val1, numAllele);
            return new Genotype(val1 + "/" + 0, variant.getReference(), variant.getAlternate());
        }

        return null;
    }

    protected void addGenotypeWithGTS(Variant variant, VariantSourceEntry sourceEntry, String[] splitsGTC,
                                      String[] alternateAlleles, int numAllele, VariantStatistics cohortStats) {
        if (sourceEntry.hasAttribute(GENOTYPE_STRING)) {
            String splitsGTS[] = sourceEntry.getAttribute(GENOTYPE_STRING).split(",");
            if (splitsGTC.length == splitsGTS.length) {
                for (int i = 0; i < splitsGTC.length; i++) {
                    String gt = splitsGTS[i];
                    int gtCount = Integer.parseInt(splitsGTC[i]);

                    Genotype g = parseGenotype(gt, variant, numAllele, alternateAlleles);
                    if (g != null) {
                        cohortStats.addGenotype(g, gtCount);
                    }
                }
            }
        }
    }

    @Override
    protected void checkVariantInformation(Variant variant, String fileId, String studyId)
            throws NonVariantException, IncompleteInformationException {
        super.checkVariantInformation(variant, fileId, studyId);

        if (requireEvidence) {
            VariantSourceEntry variantSourceEntry = variant.getSourceEntry(fileId, studyId);
            if (!canAlleleFrequenciesBeCalculated(variantSourceEntry)) {
                throw new IncompleteInformationException(variant);
            } else if (variantFrequencyIsZero(variantSourceEntry)) {
                throw new NonVariantException("The variant " + variant + " has allele frequency or counts '0'");
            }
        }
    }

    protected boolean canAlleleFrequenciesBeCalculated(VariantSourceEntry variantSourceEntry) {
        boolean frequenciesCanBeCalculated = false;
        if (variantSourceEntry.hasAttribute(ALLELE_FREQUENCY)) {
            frequenciesCanBeCalculated = true;
        } else if (variantSourceEntry.hasAttribute(ALLELE_NUMBER) && variantSourceEntry.hasAttribute(ALLELE_COUNT)) {
            frequenciesCanBeCalculated = true;
        }

        return frequenciesCanBeCalculated;
    }

    protected boolean variantFrequencyIsZero(VariantSourceEntry variantSourceEntry) {
        return isAttributeZeroInVariantSourceEntry(variantSourceEntry, ALLELE_FREQUENCY) ||
               isAttributeZeroInVariantSourceEntry(variantSourceEntry, ALLELE_COUNT) ||
               isAttributeZeroInVariantSourceEntry(variantSourceEntry, ALLELE_NUMBER);
    }

    protected boolean isAttributeZeroInVariantSourceEntry(VariantSourceEntry variantSourceEntry, String attribute) {
        return variantSourceEntry.hasAttribute(attribute) && variantSourceEntry.getAttribute(attribute).equals("0");
    }

}

