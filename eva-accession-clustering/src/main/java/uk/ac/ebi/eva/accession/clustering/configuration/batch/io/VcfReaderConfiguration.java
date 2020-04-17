/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.batch.io.CoordinatesVcfLineMapper;
import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfLineMapper;
import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.VCF_READER;

@Configuration
@Import(InputParametersConfiguration.class)
public class VcfReaderConfiguration {

    @Bean(VCF_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReader(VcfReader vcfReader) {
        return new UnwindingItemStreamReader<>(vcfReader);
    }

    @Bean
    public VcfReader vcfReader(InputParameters inputParameters) throws IOException {
        String fileId = inputParameters.getProjectAccession();
        String studyId = inputParameters.getProjectAccession();
        File vcfFile = new File(inputParameters.getVcf());
        CoordinatesVcfLineMapper lineMapper = new CoordinatesVcfLineMapper();
        return new VcfReader(lineMapper, vcfFile);
    }
}
