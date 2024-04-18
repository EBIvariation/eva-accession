package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;

@Configuration
@EnableBatchProcessing
public class AccessioningShutdownStep {
    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Bean(ACCESSIONING_SHUTDOWN_STEP)
    public Step accessioningShutDownStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("accessioningShutdownStep")
                .tasklet((contribution, chunkContext) -> {
                    submittedVariantAccessioningService.shutDownAccessionGenerator();
                    return null;
                })
                .build();
    }
}