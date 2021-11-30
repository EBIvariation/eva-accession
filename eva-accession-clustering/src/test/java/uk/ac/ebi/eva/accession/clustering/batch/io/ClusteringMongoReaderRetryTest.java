/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader.MAX_RETRIES;

@RunWith(SpringRunner.class)
public class ClusteringMongoReaderRetryTest {

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final int CHUNK_SIZE = 5;

    private ClusteringMongoReader nonClusteredVariantReader;

    @MockBean(answer = Answers.RETURNS_DEEP_STUBS)
    private MongoTemplate mongoTemplate;

    @Before
    public void setUp(){
        nonClusteredVariantReader = spy(new ClusteringMongoReader(mongoTemplate, ASSEMBLY, CHUNK_SIZE, false));
    }

    @After
    public void tearDown() {
        nonClusteredVariantReader.close();
    }

    @Test
    public void readSucceedsWhenCursorExceptionThrownOnce() {
        MongoCursor mockCursor = mock(MongoCursor.class);
        when(mockCursor.hasNext()).thenReturn(true, true, true, true, false);
        Document doc1 = new Document("id_", "1");
        Document doc2 = new Document("id_", "2");
        Document doc3 = new Document("id_", "3");
        when(mockCursor.next()).thenReturn(doc1)
                               .thenThrow(MongoCursorNotFoundException.class)
                               .thenReturn(doc2, doc3);
        openReaderWithMockCursor(mockCursor);

        List<SubmittedVariantEntity> variants = readIntoList(nonClusteredVariantReader);
        assertEquals(variants.stream().map(sve -> sve.getId()).collect(Collectors.toList()),
                     Arrays.asList("1", "2", "3"));
        // next() called once per document, plus 1 with a failure
        verify(mockCursor, times(4)).next();
        // initializeReader() called once at the start and once during the retry
        verify(nonClusteredVariantReader, times(2)).initializeReader();
    }

    @Test
    public void readFailsWhenOtherExceptionThrownOnce() {
        MongoCursor mockCursor = mock(MongoCursor.class);
        when(mockCursor.hasNext()).thenReturn(true, true, true, true, false);
        Document doc1 = new Document("id_", "1");
        Document doc2 = new Document("id_", "2");
        Document doc3 = new Document("id_", "3");
        when(mockCursor.next()).thenReturn(doc1)
                               .thenThrow(MongoException.class)
                               .thenReturn(doc2, doc3);
        openReaderWithMockCursor(mockCursor);

        assertThrows(MongoException.class, () -> readIntoList(nonClusteredVariantReader));
        // next() called twice and fails
        verify(mockCursor, times(2)).next();
        // initializeReader() called only at start (no retry)
        verify(nonClusteredVariantReader, times(1)).initializeReader();
    }

    @Test
    public void readFailsWhenCursorExceptionThrownForever() {
        MongoCursor mockCursor = mock(MongoCursor.class);
        when(mockCursor.hasNext()).thenReturn(true);
        when(mockCursor.next()).thenThrow(MongoCursorNotFoundException.class);
        openReaderWithMockCursor(mockCursor);

        assertThrows(MongoCursorNotFoundException.class, () -> readIntoList(nonClusteredVariantReader));
        // next() and initializeReader() called as many times as retry attempts allowed
        verify(mockCursor, times(MAX_RETRIES)).next();
        verify(nonClusteredVariantReader, times(MAX_RETRIES)).initializeReader();
    }

    private void openReaderWithMockCursor(MongoCursor mockCursor) {
        ExecutionContext executionContext = new ExecutionContext();

        MongoConverter mockConverter = mock(MongoConverter.class);
        when(mockConverter.read(eq(SubmittedVariantEntity.class), any(BasicDBObject.class)))
                .thenAnswer(invocation -> createSSWithId(((BasicDBObject)invocation.getArgument(1)).getString("id_")));
        when(mongoTemplate.getConverter()).thenReturn(mockConverter);
        when(mongoTemplate.getCollection(any())
                          .find(any(Bson.class))
                          .sort(any(Bson.class))
                          .noCursorTimeout(true)
                          .batchSize(any(Integer.class))
                          .iterator())
                .thenReturn(mockCursor);

        nonClusteredVariantReader.open(executionContext);
    }

    private SubmittedVariantEntity createSSWithId(String id) {
        return new SubmittedVariantEntity(1L, id, "", 1, "PRJ1", "chr1", 0, "", "", 5L, false, false, false, false, 1);
    }

    private List<SubmittedVariantEntity> readIntoList(ClusteringMongoReader reader) {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}