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

import org.apache.commons.lang3.StringUtils;

import uk.ac.ebi.eva.commons.core.models.VariantCoreFields;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class to parse the basic fields in VCF lines
 */
public abstract class VariantVcfFactory {

    public static final String ALLELE_FREQUENCY = "AF";

    public static final String ALLELE_COUNT = "AC";

    public static final String ALLELE_NUMBER = "AN";

    protected boolean includeIds = false;

    protected boolean requireEvidence = true;

    /**
     * Creates a list of Variant objects using the fields in a record of a VCF
     * file. A new Variant object is created per allele, so several of them can
     * be created from a single line.
     * <p>
     * Start/end coordinates assignment tries to work as similarly as possible
     * as Ensembl does, except for insertions, where start is greater than end:
     * http://www.ensembl.org/info/docs/tools/vep/vep_formats.html#vcf
     *
     * @param fileId,
     * @param studyId
     * @param line Contents of the line in the file
     * @return The list of Variant objects that can be created using the fields from a VCF record
     */
    public List<Variant> create(String fileId, String studyId, String line)
            throws IllegalArgumentException, NonVariantException, IncompleteInformationException {

        String[] fields = line.split("\t");
        if (fields.length < 8) {
            throw new IllegalArgumentException("Not enough fields provided (min 8)");
        }

        String chromosome = getChromosomeWithoutPrefix(fields);
        long position = getPosition(fields);
        Set<String> ids = getIds(fields);
        String reference = getReference(fields);
        String[] alternateAlleles = getAlternateAlleles(fields);

        float quality = getQuality(fields);
        String filter = getFilter(fields);
        String info = getInfo(fields);
        String format = getFormat(fields);

        List<VariantCoreFields> generatedKeyFields = buildVariantCoreFields(chromosome, position, reference,
                                                                            alternateAlleles);

        List<Variant> variants = new LinkedList<>();
        // Now create all the Variant objects read from the VCF record
        for (int altAlleleIdx = 0; altAlleleIdx < alternateAlleles.length; altAlleleIdx++) {
            VariantCoreFields keyFields = generatedKeyFields.get(altAlleleIdx);
            Variant variant = new Variant(chromosome, keyFields.getStart(), keyFields.getEnd(), keyFields.getReference(),
                                          keyFields.getAlternate());
            String[] secondaryAlternates = getSecondaryAlternates(altAlleleIdx, alternateAlleles);
            VariantSourceEntry file = new VariantSourceEntry(fileId, studyId, secondaryAlternates, format);
            variant.addSourceEntry(file);

            parseSplitSampleData(file, fields, altAlleleIdx);
            // Fill the rest of fields (after samples because INFO depends on them)
            setOtherFields(variant, fileId, studyId, ids, quality, filter, info, altAlleleIdx, alternateAlleles, line);

            checkVariantInformation(variant, fileId, studyId);

            variants.add(variant);
        }

        return variants;
    }

    /**
     * Replace "chr" references only at the beginning of the chromosome name.
     * For instance, tomato has SL2.40ch00 and that should be kept that way
     */
    protected String getChromosomeWithoutPrefix(String[] fields) {
        String chromosome = fields[0];
        boolean ignoreCase = true;
        int startOffset = 0;
        String prefixToRemove = "chr";
        if (chromosome.regionMatches(ignoreCase, startOffset, prefixToRemove, startOffset, prefixToRemove.length())) {
            return chromosome.substring(prefixToRemove.length());
        }
        return chromosome;
    }

    protected long getPosition(String[] fields) {
        return Long.parseLong(fields[1]);
    }

    protected Set<String> getIds(String[] fields) {
        if (includeIds) {
            String idsString = fields[2];
            Set<String> ids = Arrays.stream(idsString.split(";"))
                                    .filter(id -> !".".equals(id))
                                    .collect(Collectors.toSet());
            return ids;
        } else {
            return Collections.emptySet();
        }
    }

    public void setIncludeIds(boolean includeIds) {
        this.includeIds = includeIds;
    }

    public boolean isIncludeIds() {
        return includeIds;
    }

    protected String getReference(String[] fields) {
        return fields[3].equals(".") ? "" : fields[3];
    }

    protected String[] getAlternateAlleles(String[] fields) {
        return fields[4].split(",");
    }

    protected float getQuality(String[] fields) {
        return fields[5].equals(".") ? -1 : Float.parseFloat(fields[5]);
    }

    protected String getFilter(String[] fields) {
        return fields[6].equals(".") ? "" : fields[6];
    }

    protected String getInfo(String[] fields) {
        return fields[7].equals(".") ? "" : fields[7];
    }

    protected String getFormat(String[] fields) {
        return (fields.length <= 8 || fields[8].equals(".")) ? "" : fields[8];
    }

    public boolean isRequireEvidence() {
        return requireEvidence;
    }

    public void setRequireEvidence(boolean requireEvidence) {
        this.requireEvidence = requireEvidence;
    }

    private List<VariantCoreFields> buildVariantCoreFields(String chromosome, long position, String reference,
                                                           String[] alternateAlleles) {
        List<VariantCoreFields> generatedKeyFields = new ArrayList<>();

        for (int i = 0; i < alternateAlleles.length; i++) { // This index is necessary for getting the samples where
            // the mutated allele is present
            VariantCoreFields keyFields = new VariantCoreFields(chromosome, position, reference, alternateAlleles[i]);

            // Since the reference and alternate alleles won't necessarily match
            // the ones read from the VCF file but they are still needed for
            // instantiating the variants, they must be updated
            alternateAlleles[i] = keyFields.getAlternate();
            generatedKeyFields.add(keyFields);
        }
        return generatedKeyFields;
    }


    private String[] getSecondaryAlternates(int numAllele, String[] alternateAlleles) {
        String[] secondaryAlternates = new String[alternateAlleles.length - 1];
        for (int i = 0, j = 0; i < alternateAlleles.length; i++) {
            if (i != numAllele) {
                secondaryAlternates[j++] = alternateAlleles[i];
            }
        }
        return secondaryAlternates;
    }

    protected abstract void parseSplitSampleData(VariantSourceEntry variantSourceEntry, String[] fields,
                                                 int alternateAlleleIdx);


    protected void setOtherFields(Variant variant, String fileId, String studyId, Set<String> ids, float quality,
                                  String filter, String info, int numAllele, String[] alternateAlleles, String line) {
        // Fields not affected by the structure of REF and ALT fields
        if (!ids.isEmpty()) {
            variant.setMainId(ids.iterator().next());
            variant.setIds(ids);
        }
        if (quality > -1) {
            variant.getSourceEntry(fileId, studyId)
                   .addAttribute("QUAL", String.valueOf(quality));
        }
        if (!filter.isEmpty()) {
            variant.getSourceEntry(fileId, studyId).addAttribute("FILTER", filter);
        }
        if (!info.isEmpty()) {
            parseInfo(variant, fileId, studyId, info, numAllele);
        }
        variant.getSourceEntry(fileId, studyId).addAttribute("src", line);
    }

    private void parseInfo(Variant variant, String fileId, String studyId, String info, int numAllele) {
        VariantSourceEntry file = variant.getSourceEntry(fileId, studyId);

        for (String var : info.split(";")) {
            String[] splits = var.split("=");
            if (splits.length == 2) {
                switch (splits[0]) {
                    case ALLELE_COUNT:
                        String[] counts = splits[1].split(",");
                        file.addAttribute(splits[0], counts[numAllele]);
                        break;
                    case ALLELE_FREQUENCY:
                        String[] frequencies = splits[1].split(",");
                        file.addAttribute(splits[0], frequencies[numAllele]);
                        break;
                    case ALLELE_NUMBER:
                        file.addAttribute(splits[0], splits[1]);
                        break;
                    case "NS":
                        // Count the number of samples that are associated with the allele
                        file.addAttribute(splits[0], String.valueOf(file.getSamplesData().size()));
                        break;
                    case "DP":
                        int dp = 0;
                        for (Map<String, String> sampleData : file.getSamplesData()) {
                            String sampleDp = sampleData.get("DP");
                            if (StringUtils.isNumeric(sampleDp)) {
                                dp += Integer.parseInt(sampleDp);
                            }
                        }
                        file.addAttribute(splits[0], String.valueOf(dp));
                        break;
                    case "MQ":
                    case "MQ0":
                        int mq = 0;
                        int mq0 = 0;
                        for (Map<String, String> sampleData : file.getSamplesData()) {
                            String sampleGq = sampleData.get("GQ");
                            if (StringUtils.isNumeric(sampleGq)) {
                                int gq = Integer.parseInt(sampleGq);
                                mq += gq * gq;
                                if (gq == 0) {
                                    mq0++;
                                }
                            }
                        }
                        file.addAttribute("MQ", String.valueOf(mq));
                        file.addAttribute("MQ0", String.valueOf(mq0));
                        break;
                    default:
                        file.addAttribute(splits[0], splits[1]);
                        break;
                }
            } else {
                variant.getSourceEntry(fileId, studyId).addAttribute(splits[0], "");
            }
        }
    }

    /**
     * In multiallelic variants, we have a list of alternates, where numAllele is the one whose variant we are parsing
     * now. If we are parsing the first variant (numAllele == 0) A1 refers to first alternative, (i.e.
     * alternateAlleles[0]), A2 to second alternative (alternateAlleles[1]), and so on. However, if numAllele == 1, A1
     * refers to second alternate (alternateAlleles[1]), A2 to first (alternateAlleles[0]) and higher alleles remain
     * unchanged. Moreover, if NumAllele == 2, A1 is third alternate, A2 is first alternate and A3 is second alternate.
     * It's also assumed that A0 would be the reference, so it remains unchanged too.
     * <p>
     * This pattern of the first allele moving along (and swapping) is what describes this function. Also, look
     * VariantVcfFactory.getSecondaryAlternates().
     *
     * @param parsedAllele the value of parsed alleles. e.g. 1 if genotype was "A1" (first allele).
     * @param numAllele current variant of the alternates.
     * @return the correct allele index depending on numAllele.
     */
    protected static int mapToMultiallelicIndex(int parsedAllele, int numAllele) {
        int correctedAllele = parsedAllele;
        if (parsedAllele > 0) {
            if (parsedAllele == numAllele + 1) {
                correctedAllele = 1;
            } else if (parsedAllele < numAllele + 1) {
                correctedAllele = parsedAllele + 1;
            }
        }
        return correctedAllele;
    }

    protected void checkVariantInformation(Variant variant, String fileId, String studyId)
            throws NonVariantException, IncompleteInformationException {
        if (variant.getAlternate().equalsIgnoreCase(variant.getReference())) {
            throw new NonVariantException("The variant " + variant + " reference and alternate alleles are the same");
        }
        if (variant.getAlternate().equals(".")) {
            throw new NonVariantException("The variant " + variant + " has no alternate allele");
        }
    }
}
