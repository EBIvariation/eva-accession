/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets.buildReport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.util.Pair;

import uk.ac.ebi.eva.accession.pipeline.io.AccessionReportWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import static uk.ac.ebi.eva.accession.pipeline.io.AccessionReportWriter.CONTIGS_FILE_SUFFIX;
import static uk.ac.ebi.eva.accession.pipeline.io.AccessionReportWriter.VARIANTS_FILE_SUFFIX;

/**
 * Reads the 2 temporary files with contigs and variants written by {@link AccessionReportWriter} and writes a
 * complete VCF.
 *
 * This extra step is needed because the writer can not deal with resumed jobs. It could write those files and
 * concatenate them when the "close" method was called, but it is impossible to differentiate if it is being closed
 * because of an error in the batch or because all elements are finished. The temporary files should be kept in the
 * first case and deleted in the second case, so this extra step is the place to write the final VCF report and delete
 * the temporary files.
 */
public class BuildReportTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(BuildReportTasklet.class);

    private final File contigsFile;

    private final File variantsFile;

    private File output;

    public BuildReportTasklet(File output) {
        this.output = output;
        this.contigsFile = new File(output.getAbsolutePath() + CONTIGS_FILE_SUFFIX);
        this.variantsFile = new File(output.getAbsolutePath() + VARIANTS_FILE_SUFFIX);
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        writeHeader(writer, getContigPairs(contigsFile));
        appendFileToWriter(writer, variantsFile);
        writer.close();
        variantsFile.delete();
        contigsFile.delete();

        return RepeatStatus.FINISHED;
    }

    private Stream<Pair<String, String>> getContigPairs(File contigsFile) throws IOException {
        return Files.lines(contigsFile.toPath()).map(line -> {
            String[] contigColumns = line.split("\t");
            if (contigColumns.length != 2) {
                throw new IllegalStateException("Temporary file " + contigsFile.getAbsolutePath()
                                                + " doesn't have the expected format. Please delete it and "
                                                + "start a new job.");
            }
            String originalContig = contigColumns[0];
            String insdcContig = contigColumns[1];
            return Pair.of(originalContig, insdcContig);
        });
    }

    private void writeHeader(BufferedWriter writer, Stream<Pair<String, String>> inputContigAndInsdcPairs)
            throws IOException {
        writer.write("##fileformat=VCFv4.2");
        writer.newLine();

        inputContigAndInsdcPairs.forEach((Pair<String, String> inputContigAndInsdc) -> {
            try {
                writer.write("##contig=<ID=" + inputContigAndInsdc.getFirst() + ",Description=\""
                             + inputContigAndInsdc.getSecond() + "\">");
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
        writer.newLine();
    }

    private void appendFileToWriter(BufferedWriter output, File input) throws IOException {
        FileReader fileReader = new FileReader(input);
        char[] buffer = new char[4000];
        int charactersRead = fileReader.read(buffer);
        boolean endOfFileReached = charactersRead == -1;

        while (!endOfFileReached) {
            output.write(buffer, 0, charactersRead);
            charactersRead = fileReader.read(buffer);
            endOfFileReached = charactersRead == -1;
        }
    }

}
