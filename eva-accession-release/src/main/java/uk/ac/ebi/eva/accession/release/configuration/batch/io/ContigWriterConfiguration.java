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
package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import java.io.File;

import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getDbsnpActiveContigsFilePath;
import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getDbsnpMergedContigsFilePath;
import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getDbsnpMultimapContigsFilePath;
import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getEvaActiveContigsFilePath;
import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getEvaMergedContigsFilePath;
import static uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter.getEvaMultimapContigsFilePath;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_ACTIVE_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MULTIMAP_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_ACTIVE_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MULTIMAP_CONTIG_WRITER;

@Configuration
public class ContigWriterConfiguration {

    @Autowired
    private ContigMapping contigMapping;

    @Bean(DBSNP_ACTIVE_CONTIG_WRITER)
    public ContigWriter activeContigWriterDbsnp(InputParameters inputParameters) {
        return new ContigWriter(new File(getDbsnpActiveContigsFilePath(inputParameters.getOutputFolder(),
                                                                       inputParameters.getAssemblyAccession())),
                                contigMapping);
    }

    @Bean(DBSNP_MERGED_CONTIG_WRITER)
    public ContigWriter mergedContigWriterDbsnp(InputParameters inputParameters) {
        return new ContigWriter(new File(getDbsnpMergedContigsFilePath(inputParameters.getOutputFolder(),
                                                                       inputParameters.getAssemblyAccession())),
                                contigMapping);
    }

    @Bean(DBSNP_MULTIMAP_CONTIG_WRITER)
    public ContigWriter multimapContigWriterDbsnp(InputParameters inputParameters) {
        return new ContigWriter(new File(getDbsnpMultimapContigsFilePath(inputParameters.getOutputFolder(),
                                                                         inputParameters.getAssemblyAccession())),
                                contigMapping);
    }

    @Bean(EVA_ACTIVE_CONTIG_WRITER)
    public ContigWriter activeContigWriterEva(InputParameters inputParameters) {
        return new ContigWriter(new File(getEvaActiveContigsFilePath(inputParameters.getOutputFolder(),
                                                                     inputParameters.getAssemblyAccession())),
                                contigMapping);
    }

    @Bean(EVA_MERGED_CONTIG_WRITER)
    public ContigWriter mergedContigWriterEva(InputParameters inputParameters) {
        return new ContigWriter(new File(getEvaMergedContigsFilePath(inputParameters.getOutputFolder(),
                                                                     inputParameters.getAssemblyAccession())),
                                contigMapping);
    }

    @Bean(EVA_MULTIMAP_CONTIG_WRITER)
    public ContigWriter multimapContigWriterEva(InputParameters inputParameters) {
        return new ContigWriter(new File(getEvaMultimapContigsFilePath(inputParameters.getOutputFolder(),
                                                                       inputParameters.getAssemblyAccession())),
                                contigMapping);
    }
}
