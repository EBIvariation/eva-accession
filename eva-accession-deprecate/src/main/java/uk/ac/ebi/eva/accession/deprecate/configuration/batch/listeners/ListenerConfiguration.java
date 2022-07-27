/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.batch.listeners;

import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetricCompute;
import uk.ac.ebi.eva.accession.deprecate.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.batch.io.SubmittedVariantDeprecationWriter;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames;
import uk.ac.ebi.eva.accession.deprecate.batch.listeners.DeprecationStepProgressListener;
import uk.ac.ebi.eva.accession.deprecate.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.metrics.configuration.MetricConfiguration;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.util.Collections;

@Configuration
@Import({MetricConfiguration.class, InputParametersConfiguration.class})
public class ListenerConfiguration {

    @Bean(BeanNames.DEPRECATION_PROGRESS_LISTENER)
    public StepListenerSupport<SubmittedVariantEntity, SubmittedVariantEntity> deprecationProgressListener(
            SubmittedVariantDeprecationWriter submittedVariantDeprecationWriter, MetricCompute metricCompute) {
        return new DeprecationStepProgressListener(submittedVariantDeprecationWriter, metricCompute);
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(CountServiceParameters countServiceParameters,
                                                    RestTemplate restTemplate, InputParameters inputParameters) {
        return new ClusteringMetricCompute(countServiceParameters, restTemplate,
                                           inputParameters.getAssemblyAccession(),
                                           Collections.singletonList(inputParameters.getProjectAccession()));
    }
}
