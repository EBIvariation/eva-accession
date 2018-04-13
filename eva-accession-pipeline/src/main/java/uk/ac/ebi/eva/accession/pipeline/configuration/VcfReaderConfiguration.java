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
package uk.ac.ebi.eva.accession.pipeline.configuration;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.VARIANT_READER;

/**
 * Configuration to inject a VcfReader as a Variant Reader bean.
 */
@Configuration
@Import(InputParametersConfiguration.class)
public class VcfReaderConfiguration {

    @Autowired
    private InputParameters inputParameters;

    @Bean(VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReader(VcfReader vcfReader) {
        return new UnwindingItemStreamReader<>(vcfReader);
    }

    /**
     * The aggregation type is passed so that spring won't cache the instance of VcfReader if it is already built
     * with other aggregation type.
     *
     * @return a VcfReader for the given aggregation type.
     * @throws IOException if the file doesn't exist, because it has to be read to see if it's compressed.
     */
    @Bean
    @StepScope
    public VcfReader vcfReader() throws IOException {
        String fileId = inputParameters.getStudyId();
        String studyId = inputParameters.getStudyId();
        File vcfFile = new File(inputParameters.getVcf());
        Aggregation vcfAggregation = inputParameters.getVcfAggregation();

        if (Aggregation.NONE.equals(vcfAggregation)) {
            return new VcfReader(fileId, studyId, vcfFile);
        } else {
            return new AggregatedVcfReader(fileId, studyId, vcfAggregation, inputParameters.getAggregatedMappingFile(),
                    vcfFile);
        }
    }

}
