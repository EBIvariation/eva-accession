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

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigNaming;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.ContigToGenbankReplacerProcessor;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.AccessionWrapperComparator;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.IVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AccessionReportWriter {

    private static final String ACCESSION_PREFIX = "ss";

    private static final String VCF_MISSING_VALUE = ".";

    private static final String IS_HEADER_WRITTEN_KEY = "AccessionReportWriter_isHeaderWritten";

    private static final String IS_HEADER_WRITTEN_VALUE = "true";   // use string because ExecutionContext doesn't support boolean

    private static final Logger logger = LoggerFactory.getLogger(AccessionReportWriter.class);

    private static final String ORIGINAL_CHROMOSOME = "CHR";

    private final File output;

    private ContigMapping contigMapping;

    private FastaSequenceReader fastaSequenceReader;

    private BufferedWriter fileWriter;

    private String accessionPrefix;

    private ContigNaming contigNaming;

    private Set<String> loggedUnreplaceableContigs;

    public AccessionReportWriter(File output, FastaSequenceReader fastaSequenceReader, ContigMapping contigMapping,
                                 ContigNaming contigNaming) throws IOException {
        this.fastaSequenceReader = fastaSequenceReader;
        this.output = output;
        this.contigMapping = contigMapping;
        this.contigNaming = contigNaming;
        this.accessionPrefix = ACCESSION_PREFIX;
        this.loggedUnreplaceableContigs = new HashSet<>();
    }

    public String getAccessionPrefix() {
        return accessionPrefix;
    }

    public void setAccessionPrefix(String accessionPrefix) {
        this.accessionPrefix = accessionPrefix;
    }

    public void open(ExecutionContext executionContext) throws ItemStreamException {
        boolean isHeaderAlreadyWritten = IS_HEADER_WRITTEN_VALUE.equals(executionContext.get(IS_HEADER_WRITTEN_KEY));
        if (output.exists() && !isHeaderAlreadyWritten) {
            logger.warn("According to the job's execution context, the accession report should not exist, but it does" +
                                " exist. The AccessionReportWriter will append to the file, but it's possible that " +
                                "there will be 2 non-contiguous header sections in the report VCF. This can happen if" +
                                " the job execution context was not properly retrieved from the job repository.");
        }
        try {
            boolean append = true;
            this.fileWriter = new BufferedWriter(new FileWriter(this.output, append));
            if (!isHeaderAlreadyWritten) {
                writeHeader();
                executionContext.put(IS_HEADER_WRITTEN_KEY, IS_HEADER_WRITTEN_VALUE);
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    private void writeHeader() throws IOException {
        fileWriter.write("##fileformat=VCFv4.2");
        fileWriter.newLine();
        fileWriter.write("##INFO=<ID=" + ORIGINAL_CHROMOSOME +
                         ",Number=1,Type=String,Description=\"The EVA encourages the use of INSDC accessions for "
                         + "chromosomes and contigs. This field keeps track of the chromosome name as originally "
                         + "submitted\">");
        fileWriter.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
        fileWriter.newLine();
    }

    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    public void close() throws ItemStreamException {
        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    public void write(List<? extends IVariant> variants,
                      AccessionWrapperComparator accessionWrapperComparator) throws IOException {
        if (fileWriter == null) {
            throw new IOException("The file " + output + " was not opened properly. Hint: Check that the code " +
                                          "called " + this.getClass().getSimpleName() + "::open");
        }
        List<? extends IVariant> denormalizedVariants = denormalizeVariants(variants);
        denormalizedVariants.sort(accessionWrapperComparator);
        for (IVariant variant : denormalizedVariants) {
            writeVariant(variant);
        }
        fileWriter.flush();
    }

    private void writeVariant(IVariant denormalizedVariant) throws IOException {
        String contig = getEquivalentContig(denormalizedVariant.getChromosome(), contigNaming);
        Set<String> originalChromosomes = denormalizedVariant.getSourceEntries()
                                                             .stream()
                                                             .map(e -> e.getAttributes()
                                                                        .get(ContigToGenbankReplacerProcessor
                                                                                     .ORIGINAL_CHROMOSOME))
                                                             .collect(Collectors.toSet());
        if (originalChromosomes.size() != 1) {
            throw new IllegalStateException(
                    "Expected one distinct chromosome for a variant. Found: " + String.join(
                            ",", originalChromosomes) + " for variant " + denormalizedVariant);
        }

        String variantLine = String.join("\t",
                                         contig,
                                         Long.toString(denormalizedVariant.getStart()),
                                         accessionPrefix + denormalizedVariant.getMainId(),
                                         denormalizedVariant.getReference(),
                                         denormalizedVariant.getAlternate(),
                                         VCF_MISSING_VALUE, VCF_MISSING_VALUE,
                                         originalChromosomes.iterator().next());
        fileWriter.write(variantLine);
        fileWriter.newLine();
    }

    private List<? extends IVariant> denormalizeVariants(List<? extends IVariant> accessions) {
        List<? extends IVariant> denormalizedAccessions = new ArrayList<>();
        for (IVariant accession : accessions) {
            IVariant iVariant = denormalizeVariant(accession);
            Variant v = new Variant("", 0, 0, "", "");
            denormalizedAccessions.add(v);
        }
        return denormalizedAccessions;
    }

    private IVariant denormalizeVariant(IVariant normalizedVariant) {
        if (normalizedVariant.getReference().isEmpty() || normalizedVariant.getAlternate().isEmpty()) {
            if (fastaSequenceReader.doesContigExist(normalizedVariant.getChromosome())) {
                return createVariantWithContextBase(normalizedVariant);
            } else {
                throw new IllegalArgumentException("Contig '" + normalizedVariant.getChromosome()
                                                   + "' does not appear in the FASTA file ");
            }
        } else {
            return normalizedVariant;
        }
    }

    private IVariant createVariantWithContextBase(IVariant normalizedVariant) {
        String oldReference = normalizedVariant.getReference();
        String oldAlternate = normalizedVariant.getAlternate();
        long oldStart = normalizedVariant.getStart();
        ImmutableTriple<Long, String, String> startAndReferenceAndAlternate =
                fastaSequenceReader.getContextNucleotideAndNewStart(normalizedVariant.getChromosome(), oldStart,
                                                                    oldReference, oldAlternate);
        String reference = startAndReferenceAndAlternate.getMiddle();
        String alternate = startAndReferenceAndAlternate.getRight();

        Variant variant = new Variant(normalizedVariant.getChromosome(),
                                      startAndReferenceAndAlternate.getLeft(),
                                      Math.max(reference.length(), alternate.length()),
                                      reference,
                                      alternate);
        variant.setMainId(normalizedVariant.getMainId());
        variant.addSourceEntries((Collection<VariantSourceEntry>) normalizedVariant.getSourceEntries());
        return variant;

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

        String contigReplacement = contigSynonyms.get(contigNaming);
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
