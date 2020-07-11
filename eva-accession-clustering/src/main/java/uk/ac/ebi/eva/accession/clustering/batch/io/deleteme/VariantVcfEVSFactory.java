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
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Properties;

/**
 * Overrides the methods in VariantAggregatedVcfFactory that take care of the fields QUAL, FILTER and INFO, to support
 * the specific format of Exome Variant Server VCFs.
 */
public class VariantVcfEVSFactory extends VariantAggregatedVcfFactory {

    private static final String EVS_MAPPING_FILE = "/mappings/evs-mapping.properties";

    public VariantVcfEVSFactory() {
        this(null);
    }

    /**
     * @param tagMap Extends the VariantAggregatedVcfFactory(Properties properties) with one extra tag: GROUPS_ORDER.
     * Example:
     * <pre>
     * {@code
     *
     * EUR.AF=EUR_AF
     * EUR.AC=AC_EUR
     * EUR.AN=EUR_AN
     * EUR.GTC=EUR_GTC
     * ALL.AF=AF
     * ALL.AC=TAC
     * ALL.AN=AN
     * ALL.GTC=GTC
     * GROUPS_ORDER=EUR,ALL
     * }
     * </pre>
     * <p>
     * The special tag 'GROUPS_ORDER' can be used to specify the order of the comma separated values for populations in
     * tags such as MAF.
     */
    public VariantVcfEVSFactory(Properties tagMap) {
        super(tagMap);
    }

    @Override
    protected void parseStats(Variant variant, VariantSourceEntry sourceEntry, int numAllele, String[] alternateAlleles,
                              String info) {
        VariantStatistics stats = new VariantStatistics(variant);
        if (sourceEntry.hasAttribute("MAF")) {
            String splitsMAF[] = sourceEntry.getAttribute("MAF").split(",");
            if (splitsMAF.length == 3) {
                float maf = Float.parseFloat(splitsMAF[2]) / 100;
                stats.setMaf(maf);
            }
        }

        if (sourceEntry.hasAttribute(GENOTYPE_STRING) && sourceEntry.hasAttribute(GENOTYPE_COUNT)) {
            String splitsGTC[] = sourceEntry.getAttribute(GENOTYPE_COUNT).split(",");
            addGenotypeWithGTS(variant, sourceEntry, splitsGTC, alternateAlleles, numAllele, stats);
        }

        sourceEntry.setStats(stats);
    }

    @Override
    protected void parseCohortStats(Variant variant, VariantSourceEntry sourceEntry, int numAllele,
                                    String[] alternateAlleles, String info) {
        if (tagMap != null) {
            for (String key : sourceEntry.getAttributes().keySet()) {
                String opencgaTag = reverseTagMap.get(key);
                String[] values = sourceEntry.getAttribute(key).split(",");
                if (opencgaTag != null) {
                    String[] opencgaTagSplit = opencgaTag.split("\\."); // a literal point
                    if (opencgaTagSplit.length == 2) {
                        String cohort = opencgaTagSplit[0];
                        VariantStatistics cohortStats = sourceEntry.getCohortStats(cohort);
                        if (cohortStats == null) {
                            cohortStats = new VariantStatistics(variant);
                            sourceEntry.setCohortStats(cohort, cohortStats);
                        }
                        switch (opencgaTagSplit[1]) {
                            case ALLELE_COUNT:
                                cohortStats.setAltAlleleCount(Integer.parseInt(values[numAllele]));
                                cohortStats.setRefAlleleCount(Integer.parseInt(
                                        values[values.length - 1]));    // ref allele count is the last one
                                break;
                            case ALLELE_FREQUENCY:
                                cohortStats.setAltAlleleFreq(Float.parseFloat(values[numAllele]));
                                cohortStats.setRefAlleleFreq(Float.parseFloat(values[values.length - 1]));
                                break;
                            case ALLELE_NUMBER:
                                // TODO implement this. also, take into account that needed fields may not be processed yet
                                break;
                            case GENOTYPE_COUNT:
                                addGenotypeWithGTS(variant, sourceEntry, values, alternateAlleles, numAllele,
                                                   cohortStats);
                                break;
                            default:
                                break;
                        }
                    }
                } else if (key.equals("MAF")) {
                    String groups_order = tagMap.getProperty("GROUPS_ORDER");
                    if (groups_order != null) {
                        String[] populations = groups_order.split(",");
                        if (populations.length == values.length) {
                            for (int i = 0; i < values.length; i++) {   // each value has the maf of each population
                                float maf = Float.parseFloat(values[i]) / 100;  // from [0, 100] (%) to [0, 1]
                                VariantStatistics cohortStats = sourceEntry.getCohortStats(populations[i]);
                                if (cohortStats == null) {
                                    cohortStats = new VariantStatistics(variant);
                                    sourceEntry.setCohortStats(populations[i], cohortStats);
                                }
                                cohortStats.setMaf(maf);
                            }
                        }
                    }
                }
            }
            // TODO reprocess stats to complete inferable values. A StatsHolder may be needed to keep values not storables in VariantStatistics
        }
    }

    @Override
    protected boolean canAlleleFrequenciesBeCalculated(VariantSourceEntry variantSourceEntry) {
        return variantSourceEntry.hasAttribute(GENOTYPE_STRING) && variantSourceEntry.hasAttribute(GENOTYPE_COUNT);
    }

    @Override
    protected boolean variantFrequencyIsZero(VariantSourceEntry variantSourceEntry) {
        return isAttributeZeroInVariantSourceEntry(variantSourceEntry, "TAC") ||
                isAttributeZeroInVariantSourceEntry(variantSourceEntry, ALLELE_NUMBER);
    }
}

