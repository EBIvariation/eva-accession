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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

// Adapted from https://stackoverflow.com/a/42904679
public class ListOfListItemWriter<T> implements ItemWriter<List<T>> {

    private ItemWriter<T> itemWriter;

    public ListOfListItemWriter(ItemWriter<T> itemWriter) {
        this.itemWriter = itemWriter;
    }

    @Override
    public void write(List<? extends List<T>> listOfLists) throws Exception {
        if (listOfLists.isEmpty()) {
            return;
        }

        List<T> all = listOfLists.stream().flatMap(Collection::stream).collect(Collectors.toList());

        itemWriter.write(all);
    }

    public void setItemWriter(ItemStreamWriter<T> itemWriter) {
        this.itemWriter = itemWriter;
    }
}
