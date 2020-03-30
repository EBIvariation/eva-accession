/*
 * Copyright 2012 the original author or authors.
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.batch.io;

import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Mongo item reader that is based on cursors, instead of the pagination used in the default Spring Data MongoDB
 * reader.
 * <p>
 * Its implementation is based on
 * <a href="https://github.com/spring-projects/spring-batch/blob/3.0.8.RELEASE/spring-batch-infrastructure/src/main/java/org/springframework/batch/item/data/MongoItemReader.java">MongoItemReader</a>
 * but replaces the paging strategy with the use of {@link org.springframework.data.mongodb.core.MongoOperations#stream}
 * , giving better performance and still allowing an automatic conversion from MongoDB documents to the user Entity.
 *
 * Note: if you want to use this class to resume reads, make sure to provide a sort and saveState=true. If you don't
 * sort, you can't resume reads because the order might change, and the reader could skip the wrong items.
 */
public class MongoDbCursorItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
        implements InitializingBean {

    private static Logger logger = LoggerFactory.getLogger(MongoDbCursorItemReader.class);

    private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");

    private MongoOperations template;

    private String query;

    private Class<? extends T> type;

    private Sort sort;

    private String hint;

    private String fields;

    private String collection;

    private List<Object> parameterValues;

    private CloseableIterator<? extends T> cursor;

    public MongoDbCursorItemReader() {
        super();
        setName(ClassUtils.getShortName(MongoDbCursorItemReader.class));
        setSaveState(false);    // by default, assuming unsorted query. Further explanation in this class' description
    }

    /**
     * Used to perform operations against the MongoDB instance.  Also
     * handles the mapping of documents to objects.
     *
     * @param template the MongoOperations instance to use
     * @see MongoOperations
     */
    public void setTemplate(MongoOperations template) {
        this.template = template;
    }

    /**
     * A JSON formatted MongoDB query.  Parameterization of the provided query is allowed
     * via ?&lt;index&gt; placeholders where the &lt;index&gt; indicates the index of the
     * parameterValue to substitute.
     *
     * @param query JSON formatted Mongo query
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * The type of object to be returned for each {@link #read()} call.
     *
     * @param type the type of object to return
     */
    public void setTargetType(Class<? extends T> type) {
        this.type = type;
    }

    /**
     * {@link List} of values to be substituted in for each of the
     * parameters in the query.
     *
     * @param parameterValues
     */
    public void setParameterValues(List<Object> parameterValues) {
        this.parameterValues = parameterValues;
    }

    /**
     * JSON defining the fields to be returned from the matching documents
     * by MongoDB.
     *
     * @param fields JSON string that identifies the fields to sort by.
     */
    public void setFields(String fields) {
        this.fields = fields;
    }

    /**
     * {@link Map} of property names/{@link org.springframework.data.domain.Sort.Direction} values to
     * sort the input by.
     *
     * @param sorts map of properties and direction to sort each.
     */
    public void setSort(Map<String, Sort.Direction> sorts) {
        this.sort = convertToSort(sorts);
    }

    /**
     * @param collection Mongo collection to be queried.
     */
    public void setCollection(String collection) {
        this.collection = collection;
    }

    /**
     * JSON String telling MongoDB what index to use.
     *
     * @param hint string indicating what index to use.
     */
    public void setHint(String hint) {
        this.hint = hint;
    }

    @Override
    protected T doRead() throws Exception {
        return cursor.hasNext() ? cursor.next() : null;
    }

    @Override
    protected void doOpen() throws Exception {

        String populatedQuery = replacePlaceholders(query, parameterValues);

        Query mongoQuery = null;

        if(StringUtils.hasText(fields)) {
            mongoQuery = new BasicQuery(populatedQuery, fields);
        } else {
            mongoQuery = new BasicQuery(populatedQuery);
        }

        if(StringUtils.hasText(hint)) {
            mongoQuery.withHint(hint);
        }

        if (sort != null) {
            mongoQuery.with(sort);
        }

        logger.info("Issuing MongoDB query: {}", mongoQuery);

        if(StringUtils.hasText(collection)) {
            cursor = template.stream(mongoQuery, type, collection);
        } else {
            cursor = template.stream(mongoQuery, type);
        }
    }

    @Override
    protected void doClose() throws Exception {
        cursor.close();
    }

    /**
     * Checks mandatory properties
     *
     * @see InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(template != null, "An implementation of MongoOperations is required.");
        Assert.state(type != null, "A type to convert the input into is required.");
        Assert.state(query != null, "A query is required.");
    }

    // Copied from StringBasedMongoQuery...is there a place where this type of logic is already exposed?
    private String replacePlaceholders(String input, List<Object> values) {
        Matcher matcher = PLACEHOLDER.matcher(input);
        String result = input;

        while (matcher.find()) {
            String group = matcher.group();
            int index = Integer.parseInt(matcher.group(1));
            result = result.replace(group, getParameterWithIndex(values, index));
        }

        return result;
    }

    // Copied from StringBasedMongoQuery...is there a place where this type of logic is already exposed?
    private String getParameterWithIndex(List<Object> values, int index) {
        return JSON.serialize(values.get(index));
    }

    private Sort convertToSort(Map<String, Sort.Direction> sorts) {
        List<Sort.Order> sortValues = new ArrayList<Sort.Order>();

        for (Map.Entry<String, Sort.Direction> curSort : sorts.entrySet()) {
            sortValues.add(new Sort.Order(curSort.getValue(), curSort.getKey()));
        }

        return new Sort(sortValues);
    }
}
