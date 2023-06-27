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

package uk.ac.ebi.eva.accession.core.batch.io;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.util.JSON;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
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

    private MongoTemplate mongoTemplate;

    private String query;

    private Class<? extends T> type;

    private Sort sort;

    private String hint;

    private String fields;

    private String collection;

    private List<Object> parameterValues;

    private CloseableIterator<? extends T> cursor;

    private Query mongoQuery;

    private boolean startPeriodicRefreshThread = false;

    private BsonDocument serverSessionID;

    private ThreadPoolTaskScheduler scheduler;

    private ScheduledFuture periodicRefreshFuture;

    public MongoDbCursorItemReader() {
        super();
        setName(ClassUtils.getShortName(MongoDbCursorItemReader.class));
        setSaveState(false);    // by default, assuming unsorted query. Further explanation in this class' description
    }

    /**
     * Used to perform operations against the MongoDB instance.  Also
     * handles the mapping of documents to objects.
     *
     * @param mongotemplate the MongoTemplate instance to use
     * @see MongoTemplate
     */
    public void setMongoTemplate(MongoTemplate mongotemplate) {
        this.mongoTemplate = mongotemplate;
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
     * A Spring Data Query.
     * @param query
     */
    public void setQuery(Query query) {
        if (this.query != null) {
            throw new IllegalArgumentException(
                    "Only one type of query should be provided (either a String or a Query)");
        }
        this.mongoQuery = query;
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

    private void startPeriodicSessionRefresh() {
        // By default, the cursorTimeoutMillis on a Mongo server is 10 minutes
        // Therefore, try refreshing the cursors every 8 minutes (480e3 milliseconds)
        final long refreshInterval = 480000L;
        // Use Spring's thread pool scheduler so that the Spring runtime can clean up threads on exit
        this.scheduler = new ThreadPoolTaskScheduler();
        this.scheduler.setPoolSize(3);
        this.scheduler.initialize();
        this.periodicRefreshFuture = this.scheduler.scheduleAtFixedRate(() -> mongoTemplate.executeCommand(
                new Document("refreshSessions", Collections.singletonList(serverSessionID))), refreshInterval);
    }

    @Override
    protected T doRead() throws Exception {
        if (!startPeriodicRefreshThread) {
            startPeriodicSessionRefresh();
            startPeriodicRefreshThread = true;
        }
        if(cursor.hasNext()) {
            return cursor.next();
        }
        this.scheduler.shutdown();
        return null;
    }

    @Override
    protected void doOpen() throws Exception {
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder().causallyConsistent(true).build();
        ClientSession session = this.mongoTemplate.getMongoDbFactory().getSession(sessionOptions);
        this.mongoTemplate.withSession(() -> session).execute(mongoOp -> {
            this.serverSessionID = session.getServerSession().getIdentifier();
            if (mongoQuery == null) {
                String populatedQuery = replacePlaceholders(query, parameterValues);
                if (StringUtils.hasText(fields)) {
                    mongoQuery = new BasicQuery(populatedQuery, fields);
                } else {
                    mongoQuery = new BasicQuery(populatedQuery);
                }
            }

            if(StringUtils.hasText(hint)) {
                mongoQuery.withHint(hint);
            }

            if (sort != null) {
                mongoQuery.with(sort);
            }

            logger.info("Issuing MongoDB query: {}", mongoQuery);

            if(StringUtils.hasText(collection)) {
                cursor = this.mongoTemplate.stream(mongoQuery, type, collection);
            } else {
                cursor = this.mongoTemplate.stream(mongoQuery, type);
            }

            return null;
        });
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
        Assert.state(mongoTemplate != null, "An implementation of MongoTemplate is required.");
        Assert.state(type != null, "A type to convert the input into is required.");
        Assert.state(query != null || mongoQuery != null, "A query is required.");
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
