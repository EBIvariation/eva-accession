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

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DbsnpVariantAlleles {

    public static final String STR_SEQUENCE_REGEX_GROUP = "sequence";

    public static final String MICROSATELLITE_REGEX = "(?<" + STR_SEQUENCE_REGEX_GROUP + ">\\([a-zA-Z]+\\))\\d+";

    private static final Pattern SINGLE_GROUP_MICROSATELLITE_PATTERN = Pattern.compile(MICROSATELLITE_REGEX);

    private static final Pattern MULTI_GROUP_MICROSATELLITE_PATTERN = Pattern.compile("(" + MICROSATELLITE_REGEX + ")+");

    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");

    private String referenceAllele;

    private String alleles;

    private Orientation referenceOrientation;

    private Orientation allelesOrientation;

    private DbsnpVariantType dbsnpVariantType;

    public DbsnpVariantAlleles(String referenceAllele, String alleles,
                               Orientation referenceOrientation,
                               Orientation allelesOrientation,
                               DbsnpVariantType dbsnpVariantType) {
        this.referenceAllele = referenceAllele;
        this.alleles = alleles;
        this.referenceOrientation = referenceOrientation;
        this.allelesOrientation = allelesOrientation;
        this.dbsnpVariantType = dbsnpVariantType;
    }

    public String getReferenceInForwardStrand() {
        if (referenceOrientation.equals(Orientation.FORWARD)) {
            return referenceAllele;
        } else {
            return reverseComplement(referenceAllele);
        }
    }

    private String reverseComplement(String sequenceInReverseStrand) {
        StringBuilder sequenceInForwardStrand = new StringBuilder(sequenceInReverseStrand).reverse();
        for (int i = 0; i < sequenceInForwardStrand.length(); i++) {
            switch (sequenceInForwardStrand.charAt(i)) {
                // Capitalization holds a special meaning for dbSNP so we need to preserve it.
                // See https://www.ncbi.nlm.nih.gov/books/NBK44414/#_Reports_Lowercase_Small_Sequence_Letteri_
                case 'A':
                    sequenceInForwardStrand.setCharAt(i, 'T');
                    break;
                case 'a':
                    sequenceInForwardStrand.setCharAt(i, 't');
                    break;
                case 'C':
                    sequenceInForwardStrand.setCharAt(i, 'G');
                    break;
                case 'c':
                    sequenceInForwardStrand.setCharAt(i, 'g');
                    break;
                case 'G':
                    sequenceInForwardStrand.setCharAt(i, 'C');
                    break;
                case 'g':
                    sequenceInForwardStrand.setCharAt(i, 'c');
                    break;
                case 'T':
                    sequenceInForwardStrand.setCharAt(i, 'A');
                    break;
                case 't':
                    sequenceInForwardStrand.setCharAt(i, 'a');
                    break;
            }
        }
        return sequenceInForwardStrand.toString();
    }

    // TODO: return all alleles, or just the alternate ones?
    public List<String> getAllelesInForwardStrand() {
        String[] allelesArray = alleles.split("/");
        if (dbsnpVariantType.equals(DbsnpVariantType.MICROSATELLITE)) {
            return getMicrosatelliteAlleles(allelesArray);
        } else {
            if (allelesOrientation.equals(Orientation.FORWARD)) {
                return Arrays.asList(allelesArray);
            } else {
                return Arrays.stream(allelesArray).map(this::reverseComplement).collect(Collectors.toList());
            }
        }
    }

    private List<String> getMicrosatelliteAlleles(String[] allelesArray) {
        allelesArray = decodeMicrosatelliteAlleles(allelesArray);
        if (allelesOrientation.equals(Orientation.REVERSE)) {
            return Arrays.stream(allelesArray).map(this::reverseComplementMicrosatelliteAllele).collect(
                    Collectors.toList());
        } else {
            return Arrays.asList(allelesArray);
        }
    }

    /**
     * Reverse and complement the sequences contained in a microsatellite, keeping the structure.
     * I.e., (A)2(TC)8 reverse complement would be (GA)8(T)2
     * @param microsatelliteAllele Allele to reverse
     * @return Reversed and complemented allele
     */
    private String reverseComplementMicrosatelliteAllele(String microsatelliteAllele) {
        // TODO: this method reverse and complement each nucleotides group in the alleles string, but it does not
        //       reverse the group order. (A)2(TC)8 is being reversed to (T)2(GA)8 instead of (GA)8(T)2. Fix
        Matcher matcher = MULTI_GROUP_MICROSATELLITE_PATTERN.matcher(microsatelliteAllele);
        if (matcher.matches()) {
            StringBuilder complementedMicrosatelliteAllele = new StringBuilder();
            matcher = WORD_PATTERN.matcher(microsatelliteAllele);
            int charactersToCopyStart = 0;
            while (matcher.find()) {
                int charactersToCopyEnd = matcher.start();
                complementedMicrosatelliteAllele.append(microsatelliteAllele.substring(charactersToCopyStart, charactersToCopyEnd));
                String sequence = matcher.group();
                String complementedSequence = reverseComplement(sequence);
                complementedMicrosatelliteAllele.append(complementedSequence);
                charactersToCopyStart = matcher.end();
            }
            complementedMicrosatelliteAllele.append(
                    microsatelliteAllele.substring(charactersToCopyStart, microsatelliteAllele.length()));
            return complementedMicrosatelliteAllele.toString();
        } else {
            return microsatelliteAllele;
        }
    }

    /**
     * This method decode the microsatellites alleles that are just represented as a number of repetitions, adding the
     * repeated sequence. I.e., {"(AT)4","5","7"} would be decoded as {"(AT)4","(AT)5","(AT)7"}
     * @param allelesArray array that can contain alleles represented just by a number
     * @return Array where every allele has a sequence
     */
    private String[] decodeMicrosatelliteAlleles(String[] allelesArray) {
        String firstAllele = allelesArray[0];
        for (int i=1; i<allelesArray.length; i++) {
            // if a allele in the array is a number, prepend to it the sequence found in the first allele
            if (NumberUtils.isDigits(allelesArray[i])) {
                Matcher matcher = SINGLE_GROUP_MICROSATELLITE_PATTERN.matcher(firstAllele);
                if (matcher.matches()) {
                    allelesArray[i] = matcher.group(STR_SEQUENCE_REGEX_GROUP) + allelesArray[i];
                } else {
                    throw new IllegalArgumentException("Not parseable STR: " + firstAllele + "/" + allelesArray[i]);
                }
            }
        }

        return allelesArray;
    }
}
