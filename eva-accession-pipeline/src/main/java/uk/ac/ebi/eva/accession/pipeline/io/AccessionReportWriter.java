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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AccessionReportWriter {

    private static final String ACCESSION_PREFIX = "ss";

    private static final String VCF_MISSING_VALUE = ".";

    private static final String IS_HEADER_WRITTEN_KEY = "AccessionReportWriter_isHeaderWritten";

    private static final String IS_HEADER_WRITTEN_VALUE = "true";   // use string because ExecutionContext doesn't support boolean

    private static final Logger logger = LoggerFactory.getLogger(AccessionReportWriter.class);

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

    public void write(List<? extends AccessionWrapper<ISubmittedVariant, String, Long>> accessions,
                      AccessionWrapperComparator accessionWrapperComparator) throws IOException {
        if (fileWriter == null) {
            throw new IOException("The file " + output + " was not opened properly. Hint: Check that the code " +
                                          "called AccessionReportWriter::open");
        }
        List<? extends AccessionWrapper<ISubmittedVariant, String, Long>> denormalizedAccessions = denormalizeVariants(
                accessions);
        denormalizedAccessions.sort(accessionWrapperComparator);
        for (AccessionWrapper<ISubmittedVariant, String, Long> variant : denormalizedAccessions) {
            writeVariant(variant.getAccession(), variant.getData());
        }
        fileWriter.flush();
    }

    private void writeVariant(Long id, ISubmittedVariant denormalizedVariant) throws IOException {
        String vcfLine = variantToVcfLine(id, denormalizedVariant);
        fileWriter.write(vcfLine);
        fileWriter.newLine();
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

    protected String variantToVcfLine(Long id, ISubmittedVariant variant) {
        String contig = getEquivalentContig(variant, contigNaming);
        String variantLine = String.join("\t",
                                         contig,
                                         Long.toString(variant.getStart()),
                                         accessionPrefix + id,
                                         variant.getReferenceAllele(),
                                         variant.getAlternateAllele(),
                                         VCF_MISSING_VALUE, VCF_MISSING_VALUE, VCF_MISSING_VALUE);
        return variantLine;
    }

    /**
     * Note that we can't use {@link ContigToGenbankReplacerProcessor} here because we allow other replacements than
     * GenBank, while that class is used to replace to GenBank only (for writing in Mongo and for comparing input and
     * report VCFs).
     */
    private String getEquivalentContig(ISubmittedVariant normalizedVariant, ContigNaming contigNaming) {
        String oldContig = normalizedVariant.getContig();

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
