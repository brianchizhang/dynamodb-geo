package com.amazonaws.geo.s2.internal;

import com.amazonaws.geo.model.GeoQueryRequest;
import com.amazonaws.geo.model.GeoQueryRequestV2;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by mpuri on 3/28/14
 */
public class GeoQueryClientV2 {

    /**
     * The db client to use when executing the queries
     */
    private final Index dynamoTable;


    /**
     * The executor service to use to manage the queries workload
     */
    private final ExecutorService executorService;

    public GeoQueryClientV2(Index table, ExecutorService executorService) {
        this.dynamoTable = table;
        this.executorService = executorService;
    }

    /**
     * A convenience method that executes the <code>queryRequests</code> and applies the <code>resultFilter</code> to the query results.
     *
     * @return an immutable collection of filtered items
     */
    public List<Item> execute(final GeoQueryRequestV2 geoQueryRequest)
            throws InterruptedException, ExecutionException {
        final List<Item> results = new ArrayList<Item>();
        List<Future<List<Item>>> futures;
        final List<Callable<List<Item>>> queryCallables =
                new ArrayList<Callable<List<Item>>>(geoQueryRequest.getQueryRequests().size());
        for (final QuerySpec query : geoQueryRequest.getQueryRequests()) {
            queryCallables.add(new Callable<List<Item>>() {
                @Override public List<Item> call() throws Exception {
                    return executeQuery(query, geoQueryRequest.getResultFilter());
                }
            });
        }
        futures = executorService.invokeAll(queryCallables);
        if (futures != null) {
            for (Future<List<Item>> future : futures) {
                results.addAll(future.get());
            }
        }
        return ImmutableList.copyOf(results);
    }

    /**
     * Executes the  query using the provided db client. The geo filter is applied to the results of the query.
     *
     * @param queryRequest the query to execute
     * @return a collection of filtered result items
     */
    private List<Item> executeQuery(QuerySpec queryRequest, GeoFilter<Item> resultFilter) {
        ItemCollection<QueryOutcome> queryResult;
//        List<Map<String, AttributeValue>> resultItems = new ArrayList<Map<String, AttributeValue>>();

        queryResult = dynamoTable.query(queryRequest);

        List<Item> items = IteratorUtils.toList(queryResult.iterator());

        return resultFilter.filter(items);


//        do {
////            queryResult = dbClient.query(queryRequest);
//            queryResult =
//
//            List<Map<String, AttributeValue>> items = queryResult.getItems();
//            // filter the results using the geo filter
//            List<Map<String, AttributeValue>> filteredItems = resultFilter.filter(items);
//            resultItems.addAll(filteredItems);
//            queryRequest = queryRequest.withExclusiveStartKey(queryResult.getLastEvaluatedKey());
//        } while ((queryResult.getLastEvaluatedKey() != null));
//
//        return resultItems;
    }
}
