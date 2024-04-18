package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;

@Configuration
@EnableBatchProcessing
public class AccessioningShutdownStepConfiguration {
    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    @Bean(ACCESSIONING_SHUTDOWN_STEP)
    public Step accessioningShutDownStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get(ACCESSIONING_SHUTDOWN_STEP)
                .tasklet((contribution, chunkContext) -> {
                    submittedVariantAccessioningService.shutDownAccessionGenerator();
                    clusteredVariantAccessioningService.shutDownAccessionGenerator();
                    return null;
                })
                .build();
    }
}