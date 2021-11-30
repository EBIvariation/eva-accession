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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        // TODO do this properly: three variants to read
        when(mockCursor.hasNext()).thenReturn(true, true, true, true, false);
        Document doc = new Document();
        when(mockCursor.next()).thenThrow(MongoCursorNotFoundException.class)
                               .thenReturn(doc, doc, doc);
        openReaderWithMockCursor(mockCursor);

        List<SubmittedVariantEntity> variants = readIntoList(nonClusteredVariantReader);
        assertEquals(3, variants.size());
        verify(nonClusteredVariantReader, times(2)).initializeReader();
    }

    @Test
    public void readFailsWhenOtherExceptionThrownOnce() {
        MongoCursor mockCursor = mock(MongoCursor.class);
        when(mockCursor.hasNext()).thenReturn(true, true, true, true, false);
        Document doc = new Document();
        when(mockCursor.next()).thenThrow(MongoException.class)
                               .thenReturn(doc, doc, doc);
        openReaderWithMockCursor(mockCursor);

        assertThrows(MongoException.class, () -> readIntoList(nonClusteredVariantReader));
        verify(mockCursor, times(1)).next();
        verify(nonClusteredVariantReader, times(1)).initializeReader();
    }

    @Test
    public void readFailsWhenCursorExceptionThrownForever() {
        MongoCursor mockCursor = mock(MongoCursor.class);
        when(mockCursor.hasNext()).thenReturn(true);
        when(mockCursor.next()).thenThrow(MongoCursorNotFoundException.class);
        openReaderWithMockCursor(mockCursor);

        assertThrows(MongoCursorNotFoundException.class, () -> readIntoList(nonClusteredVariantReader));
        verify(mockCursor, times(5)).next();
        verify(nonClusteredVariantReader, times(5)).initializeReader();
    }

    private void openReaderWithMockCursor(MongoCursor mockCursor) {
        ExecutionContext executionContext = new ExecutionContext();

        MongoConverter mockConverter = mock(MongoConverter.class);
        when(mockConverter.read(any(), any())).thenReturn(mock(SubmittedVariantEntity.class));
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

    private List<SubmittedVariantEntity> readIntoList(ClusteringMongoReader reader) {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}