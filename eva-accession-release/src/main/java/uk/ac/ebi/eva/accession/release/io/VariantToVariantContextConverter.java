/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.io;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;

import uk.ac.ebi.eva.commons.core.models.Annotation;
import uk.ac.ebi.eva.commons.core.models.ConsequenceType;
import uk.ac.ebi.eva.commons.core.models.ConsequenceTypeMappings;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.VariantSource;
import uk.ac.ebi.eva.commons.core.models.ws.VariantSourceEntryWithSampleNames;
import uk.ac.ebi.eva.commons.core.models.ws.VariantWithSamplesAndAnnotation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VariantToVariantContextConverter {

    public static final String GENOTYPE_KEY = "GT";

    public static final String ANNOTATION_KEY = "CSQ";

    private final VariantContextBuilder variantContextBuilder;

    private List<VariantSource> sources;

    private Set<String> studies;

    private Map<String, Map<String, String>> filesSampleNamesEquivalences;

    private static final int NO_CALL_ALLELE_INDEX = 2;

    protected static final Pattern genotypePattern = Pattern.compile("/|\\|");

    private boolean excludeAnnotations;

    public VariantToVariantContextConverter(List<VariantSource> sources,
                                            Map<String, Map<String, String>> filesSampleNamesEquivalences,
                                            boolean excludeAnnotations) {
        this.sources = sources;
        this.excludeAnnotations = excludeAnnotations;
        if (sources != null) {
            this.studies = sources.stream().map(VariantSource::getStudyId).collect(Collectors.toSet());
        }
        this.filesSampleNamesEquivalences = filesSampleNamesEquivalences;
        variantContextBuilder = new VariantContextBuilder();
    }

    public VariantContext transform(VariantWithSamplesAndAnnotation variant) {
        // if there are indels, we cannot use the normalized alleles (hts forbids empty alleles), so we have to extract a context allele
        // from the VCF source line, add it to the variant and update the variant coordinates
        if (variant.getReference().isEmpty() || variant.getAlternate().isEmpty()) {
            variant = updateVariantAddingContextNucleotideFromSourceLine(variant);
        }
        String[] allelesArray = getAllelesArray(variant);

        Set<Genotype> genotypes = getGenotypes(variant, allelesArray);

        if (!excludeAnnotations) {
            String csq = getAnnotationAttributes(variant);
            if (csq != null) {
                variantContextBuilder.attribute(ANNOTATION_KEY, csq);
            }
        }

        VariantContext variantContext = variantContextBuilder
                .chr(variant.getChromosome())
                .start(variant.getStart())
                .stop(getVariantContextStop(variant))
                .noID()
                .alleles(allelesArray)
                .unfiltered()
                .genotypes(genotypes).make();
        return variantContext;
    }

    private String getAnnotationAttributes(VariantWithSamplesAndAnnotation variant) {
        Set<ConsequenceType> consequenceTypes = getConsequenceTypes(variant);
        String csq = null;
        if (consequenceTypes != null) {
            csq = consequenceTypes.stream()
                    .map(consequenceType -> transformConsequenceTypeToCsqTag(variant.getAlternate(), consequenceType))
                    .collect(Collectors.joining(","));
        }
        return csq;
    }

    private Set<ConsequenceType> getConsequenceTypes(VariantWithSamplesAndAnnotation variant) {
        Annotation annotation = variant.getAnnotation();
        Set<ConsequenceType> consequenceTypes = null;
        if (annotation != null) {
            consequenceTypes = annotation.getConsequenceTypes();
        }
        return consequenceTypes;
    }

    private String transformConsequenceTypeToCsqTag(String allele, ConsequenceType consequenceType) {
        Set<Integer> soAccessions =  consequenceType.getSoAccessions();
        String soNames = null;
        if (soAccessions != null) {
            soNames = soAccessions.stream()
                    .map(ConsequenceTypeMappings::getSoName).filter(Objects::nonNull)
                    .collect(Collectors.joining("&"));
        }
        String symbol = consequenceType.getGeneName();
        String gene = consequenceType.getEnsemblGeneId();
        String feature = consequenceType.getEnsemblTranscriptId();
        String bioType = consequenceType.getBiotype();
        Integer cDnaPosition = consequenceType.getcDnaPosition();
        Integer cdsPosition = consequenceType.getCdsPosition();

        StringBuilder csqSb = new StringBuilder();
        csqSb.append(allele != null ? allele : "").append("|")
                .append(soNames != null ? soNames : "").append("|")
                .append(symbol != null ? symbol : "").append("|")
                .append(gene != null ? gene : "").append("|")
                .append(feature != null ? feature : "").append("|")
                .append(bioType != null ? bioType : "").append("|")
                .append(cDnaPosition != null ? cDnaPosition : "").append("|")
                .append(cdsPosition != null ? cdsPosition : "");
        return csqSb.toString();
    }

    private String[] getAllelesArray(VariantWithSamplesAndAnnotation variant) {
        return new String[]{variant.getReference(), variant.getAlternate()};
    }

    private VariantWithSamplesAndAnnotation updateVariantAddingContextNucleotideFromSourceLine(VariantWithSamplesAndAnnotation variant) {
        // get the original VCF line for the variant from the 'files.src' field
        List<VariantSourceEntryWithSampleNames> studiesEntries =
                variant.getSourceEntries().stream().filter(s -> studies.contains(s.getStudyId()))
                       .collect(Collectors.toList());
        Optional<String> srcLine = studiesEntries.stream().filter(s -> s.getAttribute("src") != null).findAny()
                                                 .map(s -> s.getAttribute("src"));
        if (!srcLine.isPresent()) {
            String prefix = studiesEntries.size() == 1 ? "study " : "studies ";
            String studies = studiesEntries.stream().map(s -> s.getStudyId())
                                           .collect(Collectors.joining(",", prefix, "."));
            throw new NoSuchElementException("Source line not present for " + studies);
        }

        String[] srcLineFields = srcLine.get().split("\t", 5);

        // get the relative position of the context nucleotide in the source line REF string
        int positionInSrcLine = Integer.parseInt(srcLineFields[1]);
        // the context nucleotide is generally the one preceding the variant
        boolean prependContextNucleotideToVariant = true;
        long relativePositionOfContextNucleotide = variant.getStart() - 1 - positionInSrcLine;
        // if there is no preceding nucleotide in the source line, the context nucleotide will be "after" the variant
        if (relativePositionOfContextNucleotide < 0) {
            relativePositionOfContextNucleotide = variant.getStart() + variant.getReference()
                                                                              .length() - positionInSrcLine;
            prependContextNucleotideToVariant = false;
        }

        // get context nucleotide and add it to the variant
        String contextNucleotide = getContextNucleotideFromSourceLine(srcLineFields,
                                                                      (int) relativePositionOfContextNucleotide);
        variant = addContextNucleotideToVariant(variant, contextNucleotide, prependContextNucleotideToVariant);

        return variant;
    }


    private String getContextNucleotideFromSourceLine(String[] srcLineFields, int relativePositionOfContextNucleotide) {
        String referenceInSrcLine = srcLineFields[3];
        return referenceInSrcLine
                .substring(relativePositionOfContextNucleotide, relativePositionOfContextNucleotide + 1);
    }

    private VariantWithSamplesAndAnnotation addContextNucleotideToVariant(VariantWithSamplesAndAnnotation variant, String contextNucleotide,
                                                  boolean prependContextNucleotideToVariant) {
        VariantWithSamplesAndAnnotation newVariant;
        // prepend or append the context nucleotide to the reference and alternate alleles
        if (prependContextNucleotideToVariant) {
            // update variant start
            newVariant = new VariantWithSamplesAndAnnotation(variant.getChromosome(), variant.getStart() - 1, variant.getEnd(),
                                                             contextNucleotide + variant.getReference(),
                                                             contextNucleotide + variant.getAlternate());
            newVariant.addSourceEntries(variant.getSourceEntries());
        } else {
            // update variant end
            newVariant = new VariantWithSamplesAndAnnotation(variant.getChromosome(), variant.getStart(), variant.getEnd() + 1,
                                                             variant.getReference() + contextNucleotide,
                                                             variant.getAlternate() + contextNucleotide);
            newVariant.addSourceEntries(variant.getSourceEntries());
        }
        return newVariant;
    }

    private Set<Genotype> getGenotypes(VariantWithSamplesAndAnnotation variant, String[] allelesArray) {
        Set<Genotype> genotypes = new HashSet<>();

        Allele[] variantAlleles =
                {Allele.create(allelesArray[0], true), Allele.create(allelesArray[1]), Allele.create(Allele.NO_CALL,
                                                                                                     false)};

        for (VariantSource source : sources) {
            List<VariantSourceEntryWithSampleNames> variantStudyEntries =
                    variant.getSourceEntries().stream().filter(s -> s.getStudyId().equals(source.getStudyId()))
                           .collect(Collectors.toList());
            for (VariantSourceEntryWithSampleNames variantStudyEntry : variantStudyEntries) {
                genotypes = getStudyGenotypes(genotypes, variantAlleles, variantStudyEntry);
            }
        }
        return genotypes;
    }

    private Set<Genotype> getStudyGenotypes(Set<Genotype> genotypes, Allele[] variantAlleles,
                                            VariantSourceEntryWithSampleNames variantStudyEntry) {
        for (Map.Entry<String, Map<String, String>> sampleEntry : variantStudyEntry.getSamplesDataMap().entrySet()) {
            String sampleGenotypeString = sampleEntry.getValue().get(GENOTYPE_KEY);
            Genotype sampleGenotype =
                    parseSampleGenotype(variantAlleles, variantStudyEntry.getFileId(), sampleEntry.getKey(),
                                        sampleGenotypeString);
            genotypes.add(sampleGenotype);
        }
        return genotypes;
    }

    private Genotype parseSampleGenotype(Allele[] variantAlleles, String fileId, String sampleName,
                                         String sampleGenotypeString) {
        String[] alleles = genotypePattern.split(sampleGenotypeString, -1);
        boolean isPhased = sampleGenotypeString.contains("|");

        List<Allele> genotypeAlleles = new ArrayList<>(2);
        for (String allele : alleles) {
            int index;
            if (allele.equals(".")) {
                index = -1;
            } else {
                index = Integer.valueOf(allele);
            }
            // every allele not 0 or 1 will be considered no call
            if (index == -1 || index > NO_CALL_ALLELE_INDEX) {
                index = NO_CALL_ALLELE_INDEX;
            }
            genotypeAlleles.add(variantAlleles[index]);
        }

        GenotypeBuilder builder = new GenotypeBuilder()
                .name(getFixedSampleName(fileId, sampleName))
                .phased(isPhased)
                .alleles(genotypeAlleles);

        return builder.make();
    }

    private String getFixedSampleName(String fileId, String sampleName) {
        // this method returns the "studyId appended" sample name if there are sample name conflicts
        if (filesSampleNamesEquivalences != null) {
            return filesSampleNamesEquivalences.get(fileId).get(sampleName);
        } else {
            return sampleName;
        }
    }

    private long getVariantContextStop(IVariant variant) {
        return variant.getStart() + variant.getReference().length() - 1;
    }
}
