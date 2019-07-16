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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigNaming;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.ContigToGenbankReplacerProcessor;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.AccessionWrapperComparator;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AccessionReportWriter {

    public static final String CONTIGS_FILE_SUFFIX = ".contigs";

    public static final String VARIANTS_FILE_SUFFIX = ".variants";

    private static final String SUBSNP_ACCESSION_PREFIX = "ss";

    private static final String VCF_MISSING_VALUE = ".";

    private static final String IS_HEADER_WRITTEN_KEY = "AccessionReportWriter_isHeaderWritten";

    private static final String IS_HEADER_WRITTEN_VALUE = "true";   // use string because ExecutionContext doesn't support boolean

    private static final Logger logger = LoggerFactory.getLogger(AccessionReportWriter.class);

    private final File contigsOutput;

    private final File variantsOutput;

    private BufferedWriter contigsWriter;

    private BufferedWriter variantsWriter;

    private ContigMapping contigMapping;

    private FastaSequenceReader fastaSequenceReader;

    private String accessionPrefix;

    private ContigNaming contigNaming;

    private Set<String> loggedUnreplaceableContigs;

    private Map<String, String> inputContigsToInsdc;

    private Map<String, String> insdcToInputContigs;

    private Map<String, Set<String>> duplicatedInputContigsToInsdc;

    private Map<String, Set<String>> duplicatedInsdcToInputContigs;

    public AccessionReportWriter(File output, FastaSequenceReader fastaSequenceReader, ContigMapping contigMapping,
                                 ContigNaming contigNaming) throws IOException {
        this.fastaSequenceReader = fastaSequenceReader;
        this.contigsOutput = new File(output.getPath() + CONTIGS_FILE_SUFFIX);
        this.variantsOutput = new File(output.getPath() + VARIANTS_FILE_SUFFIX);
        this.contigMapping = contigMapping;
        this.contigNaming = contigNaming;
        this.accessionPrefix = SUBSNP_ACCESSION_PREFIX;
        this.loggedUnreplaceableContigs = new HashSet<>();
        this.inputContigsToInsdc = new HashMap<>();
        this.insdcToInputContigs = new HashMap<>();
        this.duplicatedInputContigsToInsdc = new HashMap<>();
        this.duplicatedInsdcToInputContigs = new HashMap<>();
    }

    public String getAccessionPrefix() {
        return accessionPrefix;
    }

    public void setAccessionPrefix(String accessionPrefix) {
        this.accessionPrefix = accessionPrefix;
    }

    /**
     * We do not load the variants written in previous failed executions, but there might be duplicates hard to avoid,
     * @see AccessionReportWriterTest#resumeWritingWithRepeatedVariant
     */
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        boolean isHeaderAlreadyWritten = IS_HEADER_WRITTEN_VALUE.equals(executionContext.get(IS_HEADER_WRITTEN_KEY));

        try {
            if (isHeaderAlreadyWritten) {
                // we are resuming a job
                if (contigsOutput.exists() && variantsOutput.exists()) {
                    loadContigMappingFromTemporaryFile(contigsOutput);
                    boolean append = true;
                    this.contigsWriter = new BufferedWriter(new FileWriter(this.contigsOutput, append));

                    // Not loading the variants might lead to duplicates. see this method's documentation
                    this.variantsWriter = new BufferedWriter(new FileWriter(this.variantsOutput, append));
                } else {
                    throw new IllegalStateException(
                            "Can not resume step safely. All temporary files from the previous execution ("
                            + contigsOutput.getAbsolutePath() + " and " + variantsOutput.getAbsolutePath()
                            + ") should exist to resume the step. Please delete those files and start a new job.");
                }
            } else {
                // we started a new job
                if (contigsOutput.exists() || variantsOutput.exists()) {
                    throw new IllegalStateException(
                            "A new job was started (did not resume a previous job) but temporary files exist ("
                            + contigsOutput.getAbsolutePath() + " or " + variantsOutput.getAbsolutePath()
                            + "). Please delete them and start a new job");
                } else {
                    boolean append = false;
                    this.contigsWriter = new BufferedWriter(new FileWriter(this.contigsOutput, append));
                    this.variantsWriter = new BufferedWriter(new FileWriter(this.variantsOutput, append));
                    executionContext.put(IS_HEADER_WRITTEN_KEY, IS_HEADER_WRITTEN_VALUE);
                }
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    /**
     * Loads contig replacement mapping from a previous execution to avoid writing duplicate contig entries in the final
     * VCF.
     */
    private void loadContigMappingFromTemporaryFile(File contigMappingFile) throws IOException {
        BufferedReader contigReader = new BufferedReader(new FileReader(contigMappingFile));
        String line;
        while ((line = contigReader.readLine()) != null) {
            String[] contigColumns = line.split("\t");
            if (contigColumns.length != 2) {
                throw new IllegalStateException("Temporary file " + contigMappingFile.getAbsolutePath()
                                                + " doesn't have the expected format. Please delete it and "
                                                + "start a new job.");
            }
            inputContigsToInsdc.put(contigColumns[0], contigColumns[1]);
            insdcToInputContigs.put(contigColumns[1], contigColumns[0]);
        }
    }

    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    public void close() throws ItemStreamException {
        try {
            contigsWriter.close();
            variantsWriter.close();
            if (!duplicatedInputContigsToInsdc.isEmpty()) {
                logger.error(
                        "The same chromosome (in the original input) was replaced by several INSDC contig accessions. "
                        + "This happened for: " + duplicatedInputContigsToInsdc.toString());
            }
            if (!duplicatedInsdcToInputContigs.isEmpty()) {
                logger.error("The same INSDC contig accessions replaced several input chromosomes. This happened for: "
                             + duplicatedInsdcToInputContigs.toString());
            }
            if (!duplicatedInputContigsToInsdc.isEmpty() || !duplicatedInsdcToInputContigs.isEmpty()) {
                throw new IllegalStateException(
                        "Contig replacement was done but the replacements were not unique. See errors above for "
                        + "details");
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    public void write(List<? extends IVariant> originalVariantsWithInsdcContigs,
                      List<AccessionWrapper<ISubmittedVariant, String, Long>> accessionedVariants) throws IOException {
        if (variantsWriter == null) {
            throw new IOException("The file " + variantsOutput + " was not opened properly. Hint: Check that the code "
                                  + "called " + this.getClass().getSimpleName() + "::open");
        }
        updateChromosomeMappings(originalVariantsWithInsdcContigs);
        List<? extends AccessionWrapper<ISubmittedVariant, String, Long>> denormalizedVariants = denormalizeVariants(
                accessionedVariants);
        denormalizedVariants.sort(new AccessionWrapperComparator(originalVariantsWithInsdcContigs));
        for (AccessionWrapper<ISubmittedVariant, String, Long> variant : denormalizedVariants) {
            writeSortedVariant(variant, insdcToInputContigs);
        }
        variantsWriter.flush();
    }

    private void updateChromosomeMappings(List<? extends IVariant> originalVariantsWithReplacedContigs)
            throws IOException {
        for (IVariant variantWithContig : originalVariantsWithReplacedContigs) {
            String originalChromosome = getOriginalChromosome(variantWithContig);
            String currentInsdcContig = variantWithContig.getChromosome();

            String previousInsdcReplacementForOriginalChromosome = inputContigsToInsdc.put(originalChromosome,
                                                                                           currentInsdcContig);
            if (previousInsdcReplacementForOriginalChromosome == null) {
                // there was no previous entry, so this is not present in the contigs file: write it
                contigsWriter.write(originalChromosome + "\t" + currentInsdcContig);
                contigsWriter.newLine();
                contigsWriter.flush();
            } else if (!previousInsdcReplacementForOriginalChromosome.equals(currentInsdcContig)) {
                Set<String> contigsAssociatedToTheSameChromosome = duplicatedInputContigsToInsdc.computeIfAbsent(
                        originalChromosome, key -> new HashSet<>());
                contigsAssociatedToTheSameChromosome.add(currentInsdcContig);
                contigsAssociatedToTheSameChromosome.add(previousInsdcReplacementForOriginalChromosome);
            }

            String previousChromosome = insdcToInputContigs.put(currentInsdcContig, originalChromosome);
            if (previousChromosome != null && !previousChromosome.equals(originalChromosome)) {
                Set<String> chromosomesAssociatedToTheSameContig = duplicatedInsdcToInputContigs.computeIfAbsent(
                        currentInsdcContig, key -> new HashSet<>());
                chromosomesAssociatedToTheSameContig.add(originalChromosome);
                chromosomesAssociatedToTheSameContig.add(previousChromosome);
            }
        }
    }

    private String getOriginalChromosome(IVariant variant) {
        Set<String> originalChromosomes = variant.getSourceEntries()
                                                 .stream()
                                                 .map(se -> se.getAttributes().get(
                                                         ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME))
                                                 .collect(Collectors.toSet());

        if (originalChromosomes.size() != 1) {
            throw new IllegalStateException(
                    "Bug detected: Multiple original chromosomes were found to be associated with the same variant "
                    + variant.toString() + ". The attributes had the next list of original chromosomes: ["
                    + String.join(", ", originalChromosomes) + "]. Contig '" + variant.getChromosome()
                    + "' was used as replacement.");
        }
        return originalChromosomes.iterator().next();
    }

    private List<? extends AccessionWrapper<ISubmittedVariant, String, Long>> denormalizeVariants(
            List<? extends AccessionWrapper<ISubmittedVariant, String, Long>> accessions) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> denormalizedAccessions = new ArrayList<>();
        for (AccessionWrapper<ISubmittedVariant, String, Long> accession : accessions) {
            denormalizedAccessions.add(new AccessionWrapper<>(accession.getAccession(), accession.getHash(),
                                                              denormalizeVariant(accession.getData())));
        }
        return denormalizedAccessions;
    }

    private ISubmittedVariant denormalizeVariant(ISubmittedVariant normalizedVariant) {
        if (normalizedVariant.getReferenceAllele().isEmpty() || normalizedVariant.getAlternateAllele().isEmpty()) {
            if (fastaSequenceReader.doesContigExist(normalizedVariant.getContig())) {
                return createVariantWithContextBase(normalizedVariant);
            } else {
                throw new IllegalArgumentException("Contig '" + normalizedVariant.getContig()
                                                   + "' does not appear in the FASTA file ");
            }
        } else {
            return normalizedVariant;
        }
    }

    private ISubmittedVariant createVariantWithContextBase(ISubmittedVariant normalizedVariant) {
        String oldReference = normalizedVariant.getReferenceAllele();
        String oldAlternate = normalizedVariant.getAlternateAllele();
        long oldStart = normalizedVariant.getStart();
        ImmutableTriple<Long, String, String> contextNucleotideInfo =
                fastaSequenceReader.getContextNucleotideAndNewStart(normalizedVariant.getContig(), oldStart,
                                                                    oldReference, oldAlternate);

        return new SubmittedVariant(normalizedVariant.getReferenceSequenceAccession(),
                                    normalizedVariant.getTaxonomyAccession(),
                                    normalizedVariant.getProjectAccession(),
                                    normalizedVariant.getContig(),
                                    contextNucleotideInfo.getLeft(),
                                    contextNucleotideInfo.getMiddle(),
                                    contextNucleotideInfo.getRight(),
                                    normalizedVariant.getClusteredVariantAccession(),
                                    normalizedVariant.isSupportedByEvidence(),
                                    normalizedVariant.isAssemblyMatch(),
                                    normalizedVariant.isAllelesMatch(),
                                    normalizedVariant.isValidated(),
                                    normalizedVariant.getCreatedDate());

    }

    /**
     * Replace the contig using the requested contig naming and write the variant to the output file.
     *
     * Note how this is done after the sorting (using {@link AccessionWrapperComparator}) because the mappings we
     * passed to it are mappings from the input naming to INSDC, not from input naming to requested output naming.
     *
     * Also, we have to get the equivalent of the original chromosome (not the INSDC contig) because we will mostly use
     * {@link ContigNaming#NO_REPLACEMENT} and it means "do not replace the submitter's original chromosome".
     */
    private void writeSortedVariant(AccessionWrapper<ISubmittedVariant, String, Long> denormalizedVariant,
                                    Map<String, String> insdcToInputContigs) throws IOException {
        String originalChromosome = insdcToInputContigs.get(denormalizedVariant.getData().getContig());
        String contigFromRequestedContigNaming = getEquivalentContig(originalChromosome, contigNaming);

        String variantLine = String.join("\t",
                                         contigFromRequestedContigNaming,
                                         Long.toString(denormalizedVariant.getData().getStart()),
                                         accessionPrefix + denormalizedVariant.getAccession(),
                                         denormalizedVariant.getData().getReferenceAllele(),
                                         denormalizedVariant.getData().getAlternateAllele(),
                                         VCF_MISSING_VALUE, VCF_MISSING_VALUE, VCF_MISSING_VALUE);
        variantsWriter.write(variantLine);
        variantsWriter.newLine();
    }

    /**
     * Note that we can't use {@link ContigToGenbankReplacerProcessor} here because we allow other replacements than
     * GenBank, while that class is used to replace to GenBank only (for writing in Mongo and for comparing input and
     * report VCFs).
     */
    private String getEquivalentContig(String oldContig, ContigNaming contigNaming) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(oldContig);
        if (contigSynonyms == null) {
            if (!loggedUnreplaceableContigs.contains(oldContig)) {
                loggedUnreplaceableContigs.add(oldContig);
                logger.warn("Will not replace contig '" + oldContig
                            + "' (in the current variant or any subsequent one) as requested because there are no "
                            + "synonyms available. (Hint: Is the assembly report correct and complete?)");
            }
            return oldContig;
        }

        String contigReplacement = contigMapping.getContigSynonym(oldContig, contigSynonyms, contigNaming);
        if (contigReplacement == null) {
            if (!loggedUnreplaceableContigs.contains(oldContig)) {
                loggedUnreplaceableContigs.add(oldContig);
                logger.warn("Will not replace contig '" + oldContig
                            + "' (in the current variant or any subsequent one) as requested because there is no "
                            + contigNaming + " synonym for it.");
            }
            return oldContig;
        }

        boolean genbankReplacedWithRefseq = oldContig.equals(contigSynonyms.getGenBank())
                                            && contigReplacement.equals(contigSynonyms.getRefSeq());

        boolean refseqReplacedWithGenbank = oldContig.equals(contigSynonyms.getRefSeq())
                                            && contigReplacement.equals(contigSynonyms.getGenBank());

        if (!contigSynonyms.isIdenticalGenBankAndRefSeq() && (genbankReplacedWithRefseq || refseqReplacedWithGenbank)) {
            if (!loggedUnreplaceableContigs.contains(oldContig)) {
                loggedUnreplaceableContigs.add(oldContig);
                logger.warn(
                        "Will not replace contig '" + oldContig + "' with " + contigNaming + " '" + contigReplacement
                        + "' (in the current variant or any subsequent one) as requested because those contigs "
                        + "are not identical according to the assembly report provided.");
            }
            return oldContig;
        }

        return contigReplacement;
    }

}
