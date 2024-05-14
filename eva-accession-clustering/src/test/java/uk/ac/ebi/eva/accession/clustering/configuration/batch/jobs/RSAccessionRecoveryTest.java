package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.entities.ContiguousIdBlock;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.eva.accession.clustering.test.configuration.RSAccessionRecoveryTestConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.RSAccessionRecoveryTestConfiguration.JOB_LAUNCHER_RS_ACCESSION_RECOVERY;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {RSAccessionRecoveryTestConfiguration.class,})
@TestPropertySource("classpath:rs-accession-recovery.properties")
@SpringBatchTest
public class RSAccessionRecoveryTest {
    @Autowired
    private ContiguousIdBlockRepository blockRepository;

    @Autowired
    private ClusteredVariantAccessioningRepository mongoRepository;

    @Autowired
    @Qualifier(JOB_LAUNCHER_RS_ACCESSION_RECOVERY)
    private JobLauncherTestUtils jobLauncherTestUtilsMonotonicAccessionRecoveryAgent;

    @Test
    @DirtiesContext
    public void testContiguousBlocksForCategoryRSAreRecovered() throws Exception {
        initializeMongoDbWithUncommittedAccessions();
        verifyInitialDBState();

        // recovery cut off time is -14 days (provided in rs-accession-recovery.properties)
        JobExecution jobExecution = jobLauncherTestUtilsMonotonicAccessionRecoveryAgent.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        verifyEndDBState();
    }

    private void initializeMongoDbWithUncommittedAccessions() {
        mongoRepository.deleteAll();

        List<ClusteredVariantEntity> clusteredVariantEntityList = new ArrayList<>();
        // Entries for 1st block
        for (long i = 3000000000l; i <= 3000000029l; i++) {
            ClusteredVariant model = new ClusteredVariant("assembly", 1111,
                    "contig", 100, VariantType.SNV, false, LocalDateTime.now());
            ClusteredVariantEntity entity = new ClusteredVariantEntity(i, "hash" + i, model, 1);
            clusteredVariantEntityList.add(entity);
        }

        // Entries for 2nd block - Missing 5 RS (3000000035l - 3000000039l)
        for (long i = 3000000030l; i <= 3000000034l; i++) {
            ClusteredVariant model = new ClusteredVariant("assembly", 1111,
                    "contig", 100, VariantType.SNV, false, LocalDateTime.now());
            ClusteredVariantEntity entity = new ClusteredVariantEntity(i, "hash" + i, model, 1);
            clusteredVariantEntityList.add(entity);
        }
        for (long i = 3000000040l; i <= 3000000059l; i++) {
            ClusteredVariant model = new ClusteredVariant("assembly", 1111,
                    "contig", 100, VariantType.SNV, false, LocalDateTime.now());
            ClusteredVariantEntity entity = new ClusteredVariantEntity(i, "hash" + i, model, 1);
            clusteredVariantEntityList.add(entity);
        }

        // Entries for 3rd block
        for (long i = 3000000060l; i <= 3000000089l; i++) {
            ClusteredVariant model = new ClusteredVariant("assembly", 1111,
                    "contig", 100, VariantType.SNV, false, LocalDateTime.now());
            ClusteredVariantEntity entity = new ClusteredVariantEntity(i, "hash" + i, model, 1);
            clusteredVariantEntityList.add(entity);
        }

        // Entries for 5th block
        for (long i = 3000000120l; i <= 3000000129l; i++) {
            ClusteredVariant model = new ClusteredVariant("assembly", 1111,
                    "contig", 100, VariantType.SNV, false, LocalDateTime.now());
            ClusteredVariantEntity entity = new ClusteredVariantEntity(i, "hash" + i, model, 1);
            clusteredVariantEntityList.add(entity);
        }

        mongoRepository.saveAll(clusteredVariantEntityList);
    }

    private void verifyInitialDBState() {
        // Initial state of Contiguous Id Block DB is 5 blocks are present but their "last_committed" is not updated
        // (Initialized using "resources/test-data/rs_accession_recovery_test_data.sql")

        // block id  first value  last value  last committed  reserved  last_updated_timestamp | remarks
        //  1        3000000000   3000000029  2999999999      true      1970-01-01 00:00:00    | should be recovered
        //  2        3000000030   3000000059  3000000029      true      1970-01-01 00:00:00    | should be recovered
        //  3        3000000060   3000000089  3000000059      true      1970-01-01 00:00:00    | should be recovered
        //  4        3000000090   3000000119  3000000089      true      1970-01-01 00:00:00    | should be recovered
        //  5        3000000120   3000000149  3000000119      true      2099-01-01 00:00:00    | should not be recovered

        // Mongo DB
        // 95 accessions have been used in mongoDB but are not reflected in the block allocation table
        // 30 accessions belong to 1st block (3000000000 to 3000000029),
        // 25 to the 2nd block (3000000030 to 300000034 and 3000000040 to 3000000059)
        // 30 to the 3rd block (3000000060 to 3000000089)
        // None in 4th block
        // 10 to the 5th block (3000000120 to 3000000129)

        assertEquals(95, mongoRepository.count());   // 30 + 25 + 30 + 10
        assertEquals(5, blockRepository.count());

        // Since none of the 4 blocks got committed - everyone's last committed value is first_value - 1
        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(3000000000l, block1.getFirstValue());
        assertEquals(2999999999l, block1.getLastCommitted());
        assertEquals(3000000029l, block1.getLastValue());
        assertEquals("test-instance-recover-state-00", block1.getApplicationInstanceId());
        assertTrue(block1.isReserved());

        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(3000000030l, block2.getFirstValue());
        assertEquals(3000000029l, block2.getLastCommitted());
        assertEquals(3000000059l, block2.getLastValue());
        assertEquals("test-instance-recover-state-00", block2.getApplicationInstanceId());
        assertTrue(block2.isReserved());

        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(3000000060l, block3.getFirstValue());
        assertEquals(3000000059l, block3.getLastCommitted());
        assertEquals(3000000089l, block3.getLastValue());
        assertEquals("test-instance-recover-state-00", block3.getApplicationInstanceId());
        assertTrue(block3.isReserved());

        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(3000000090l, block4.getFirstValue());
        assertEquals(3000000089l, block4.getLastCommitted());
        assertEquals(3000000119l, block4.getLastValue());
        assertEquals("test-instance-recover-state-00", block4.getApplicationInstanceId());
        assertTrue(block4.isReserved());

        ContiguousIdBlock block5 = blockRepository.findById(5l).get();
        assertEquals(3000000120l, block5.getFirstValue());
        assertEquals(3000000119l, block5.getLastCommitted());
        assertEquals(3000000149l, block5.getLastValue());
        assertEquals("test-instance-recover-state-00", block5.getApplicationInstanceId());
        assertTrue(block5.isReserved());
    }

    private void verifyEndDBState() {
        assertEquals(5, blockRepository.count());

        // Block Recovered - recovered entire block
        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(3000000000l, block1.getFirstValue());
        assertEquals(3000000029l, block1.getLastCommitted());
        assertEquals(3000000029l, block1.getLastValue());
        assertEquals("0", block1.getApplicationInstanceId());
        assertTrue(block1.isNotReserved());

        // Block Recovered partially - (used 3000000030-3000000034 and 3000000040-3000000059)
        // since there are unused accessions(3000000035-3000000039), last_committed is updated to 3000000034 only)
        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(3000000030l, block2.getFirstValue());
        assertEquals(3000000034l, block2.getLastCommitted());
        assertEquals(3000000059l, block2.getLastValue());
        assertEquals("0", block2.getApplicationInstanceId());
        assertTrue(block2.isNotReserved());

        // Block Recovered - recovered entire block
        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(3000000060l, block3.getFirstValue());
        assertEquals(3000000089l, block3.getLastCommitted());
        assertEquals(3000000089l, block3.getLastValue());
        assertEquals("0", block3.getApplicationInstanceId());
        assertTrue(block3.isNotReserved());

        // Block Recovered - None of the accessions are used, just released the block
        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(3000000090l, block4.getFirstValue());
        assertEquals(3000000089l, block4.getLastCommitted());
        assertEquals(3000000119l, block4.getLastValue());
        assertEquals("0", block4.getApplicationInstanceId());
        assertTrue(block4.isNotReserved());

        // Block Not Recovered - Block not to be recovered as it's last update is after cut off time
        ContiguousIdBlock block5 = blockRepository.findById(5l).get();
        assertEquals(3000000120l, block5.getFirstValue());
        assertEquals(3000000119l, block5.getLastCommitted());
        assertEquals(3000000149l, block5.getLastValue());
        assertEquals("test-instance-recover-state-00", block5.getApplicationInstanceId());
        assertTrue(block5.isReserved());
    }
}