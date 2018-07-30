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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DbsnpVariantAlleles {

    private static final String STR_MOTIF_REGEX_GROUP = "motif";

    private static final String BRACKETED_STR_MOTIF_REGEX_GROUP = "bracketedMotif";

    /** This Regex captures a motif in a STR expression, i.e., 'GA' in '(GA)5' */
    private static final String MOTIF_GROUP_REGEX = "(?<" + STR_MOTIF_REGEX_GROUP + ">[a-zA-Z]+)";

    /** This regex captures a bracketed motif in a STR expression, i.e., '(GA)' in '(GA)5' */
    private static final String BRACKETED_MOTIF_GROUP_REGEX = "(?<" + BRACKETED_STR_MOTIF_REGEX_GROUP + ">" +
        "\\(" + MOTIF_GROUP_REGEX + "\\))";

    /** Regular expresion that captures a STR expression, like '(GA)5' */
    private static final String STR_UNIT_REGEX = BRACKETED_MOTIF_GROUP_REGEX + "\\d*";

    private static final Pattern STR_UNIT_PATTERN = Pattern.compile(STR_UNIT_REGEX);

    private String referenceAllele;

    private String[] alleles;

    private Orientation referenceOrientation;

    private Orientation allelesOrientation;

    private DbsnpVariantType dbsnpVariantType;

    public DbsnpVariantAlleles(String referenceAllele, String alleles,
                               Orientation referenceOrientation,
                               Orientation allelesOrientation,
                               DbsnpVariantType dbsnpVariantType) {
        this.referenceAllele = getTrimmedAllele(referenceAllele);
        this.alleles = splitAndTrimAlleles(alleles);
        this.referenceOrientation = referenceOrientation;
        this.allelesOrientation = allelesOrientation;
        this.dbsnpVariantType = dbsnpVariantType;
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

    private String[] splitAndTrimAlleles(String alleles) {
        return Arrays.stream(StringUtils.split(alleles, "/")).map(DbsnpVariantAlleles::getTrimmedAllele).
                toArray(String[]::new);
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

    public List<String> getAllelesInForwardStrand() {
        if (dbsnpVariantType.equals(DbsnpVariantType.MICROSATELLITE)) {
            return getMicrosatelliteAllelesInForwardStrand();
        } else {
            if (allelesOrientation.equals(Orientation.FORWARD)) {
                return Arrays.asList(alleles);
            } else {
                return Arrays.stream(alleles).map(this::reverseComplement).collect(Collectors.toList());
            }
        }
    }

    /** This method return a list containing each allele in a Microsatellite type variant, reversing and complementig
     * them if necessary
     * @return List containing all alleles in the forward strand
     */
    private List<String> getMicrosatelliteAllelesInForwardStrand() {
        String[] allelesArray = removeSurrondingSquareBrackets(alleles);
        allelesArray = decodeMicrosatelliteAlleles(allelesArray);
        if (allelesOrientation.equals(Orientation.REVERSE)) {
            return Arrays.stream(allelesArray).map(this::reverseComplementMicrosatelliteSequence).collect(
                    Collectors.toList());
        } else {
            return Arrays.asList(allelesArray);
        }
    }

    /**
     * There are some dbSNP STR variant alleles that are surronded by square brackets. This method removes them so the
     * variant can be parsed correctly
     * @param allelesArray Array containing all alleles
     * @return Array containing all alleles, where the surronding square brackets have been removed
     */
    private String[] removeSurrondingSquareBrackets(String[] allelesArray) {
        if (allelesArray[0].charAt(0) == '[') {
            // All the STR patterns have been checked, and when the first character is a opening square bracket,
            // the closing one is the last one
            allelesArray[0] = allelesArray[0].substring(1);
            int lastAlleleIndex = allelesArray.length -1;
            allelesArray[lastAlleleIndex] = allelesArray[lastAlleleIndex].substring(0, allelesArray[lastAlleleIndex]
                    .length() - 1);
        }
        return allelesArray;
    }

    /**
     * This method decode the microsatellites alleles that are just represented as a number of repetitions, adding the
     * repeated motif. I.e., {"(AT)4","5","7"} would be decoded as {"(AT)4","(AT)5","(AT)7"}
     * @param allelesArray array that can contain alleles represented just by a number
     * @return Array where every allele has a sequence
     */
    private String[] decodeMicrosatelliteAlleles(String[] allelesArray) {
        String firstAllele = allelesArray[0];
        for (int i=1; i<allelesArray.length; i++) {
            // if a allele in the array is a number, prepend to it the sequence found in the first allele
            if (NumberUtils.isDigits(allelesArray[i])) {
                Matcher matcher = STR_UNIT_PATTERN.matcher(firstAllele);
                if (matcher.matches()) {
                    allelesArray[i] = matcher.group(BRACKETED_STR_MOTIF_REGEX_GROUP) + allelesArray[i];
                } else {
                    throw new IllegalArgumentException("Not parseable STR: " + firstAllele + "/" + allelesArray[i]);
                }
            }
        }

        return allelesArray;
    }

    /**
     * Reverse and complement a sequence represented by one or several STRs
     * I.e., (A)2(TC)8 reverse complement would be (GA)8(T)2 (the order of the STRs is also inverted)
     * @param microsatellite STR sequence to reverse
     * @return Reversed and complemented microsatellite
     */
    private String reverseComplementMicrosatelliteSequence(String microsatellite) {
        // we have to reverse the order of the groups in the microsatellite, so we add them to a stack
        Deque<String> stack = new ArrayDeque<>();
        Matcher matcher = STR_UNIT_PATTERN.matcher(microsatellite);
        if (matcher.groupCount() > 0) {
            while (matcher.find()) {
                String microsatelliteUnit = matcher.group();
                stack.addFirst(microsatelliteUnit);
            }
        } else {
            // this allele has no STRs, so we add the full sequence to the stack
            stack.addFirst(microsatellite);
        }

        // each STR string in the stack is now reversed/complemented, and added to the output string
        StringBuilder reversedComplementedAllele = new StringBuilder();
        for (String microsatelliteUnit : stack) {
            reversedComplementedAllele.append(reverseComplementMicrosatelliteUnit(microsatelliteUnit));
        }

        return reversedComplementedAllele.toString();
    }

    /**
     * This method reverses and complement a single microsatellite 'unit'. Each unit in a STR expression is a sequence
     * motif and the number of times is repeated. I.e., the microsatellite "(AG)5(C)4" has the units "(AG)5" and "(C)4".
     * The reverse complement for the unit "(AG)5" will be "(CT)5" (notice that each allele has been complemented but
     * the sequence has been reversed also)
     * @param str microsatellite unit to reverse and complement
     * @return reversed complemented microsatellite unit
     */
    private String reverseComplementMicrosatelliteUnit(String str) {
        Matcher matcher = STR_UNIT_PATTERN.matcher(str);
        if (matcher.matches()) {
            String sequence = matcher.group(STR_MOTIF_REGEX_GROUP);
            return str.replaceFirst(sequence, reverseComplement(sequence));
        } else {
            return reverseComplement(str);
        }
    }
}
