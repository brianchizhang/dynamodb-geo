package com.amazonaws.geo;

import com.amazonaws.geo.model.*;
import com.amazonaws.geo.model.filters.GeoFilters;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import com.dashlabs.dash.geo.s2.internal.S2Manager;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.base.Optional;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by mpuri on 3/24/14
 */
public class Geo {

    private final S2Manager s2Manager;

    private final GeoQueryHelper geoQueryHelper;

    public Geo() {
        this.s2Manager = new S2Manager();
        this.geoQueryHelper = new GeoQueryHelper(s2Manager);
    }

    public Geo(S2Manager s2Manager, GeoQueryHelper geoQueryHelper) {
        this.s2Manager = s2Manager;
        this.geoQueryHelper = geoQueryHelper;
    }

    /**
     * Decorates the given <code>putItemRequest</code> with attributes required for geo spatial querying.
     *
     * @param putItemRequest the request that needs to be decorated with geo attributes
     * @param latitude       the latitude that needs to be attached with the item
     * @param longitude      the longitude that needs to be attached with the item
     * @param configs        the collection of configurations to be used for decorating the request with geo attributes
     * @return the decorated request
     */
    public PutItemRequest putItemRequest(PutItemRequest putItemRequest, double latitude, double longitude, List<GeoConfig> configs) {
        updateAttributeValues(putItemRequest.getItem(), latitude, longitude, configs);
        return putItemRequest;
    }

//    public Item putItemRequest(Item item, double latitude, double longitude, List<GeoConfig> configs) {
//        updateAttributeValues(item, latitude, longitude, configs);
//    }

    /**
     * Decorates the given <code>updateItemRequest</code> with attributes required for geo spatial querying.
     *
     * @param attributeValueMap the items that needs to be decorated with geo attributes
     * @param latitude          the latitude that needs to be attached with the item
     * @param longitude         the longitude that needs to be attached with the item
     * @param configs           the collection of configurations to be used for decorating the request with geo attributes
     */
    public void updateAttributeValues(Map<String, AttributeValue> attributeValueMap, double latitude, double longitude,
                                                   List<GeoConfig> configs) {
        if (configs == null) {
            throw new IllegalArgumentException("Geo configs should not be null");
        }
        for (GeoConfig config : configs) {
            //Fail-fast if any of the preconditions fail
            checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(),
                    config.getGeoHashKeyLength());

            long geohash = s2Manager.generateGeohash(latitude, longitude);
            long geoHashKey = s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength());

            //Decorate the request with the geohash
            AttributeValue geoHashValue = new AttributeValue().withN(Long.toString(geohash));
            attributeValueMap.put(config.getGeoHashColumn(), geoHashValue);

            AttributeValue geoHashKeyValue;
            if (config.getHashKeyDecorator().isPresent() && config.getCompositeHashKeyColumn().isPresent()) {
                AttributeValue compositeHashKeyValue = attributeValueMap.get(config.getCompositeHashKeyColumn().get());
                if (compositeHashKeyValue == null) {
                    continue;
                }
                String compositeColumnValue = compositeHashKeyValue.getS();
                String hashKey = config.getHashKeyDecorator().get().decorate(compositeColumnValue, geoHashKey);
                //Decorate the request with the composite geoHashKey (type String)
                geoHashKeyValue = new AttributeValue().withS(String.valueOf(hashKey));
            } else {
                //Decorate the request with the geoHashKey (type Number)
                geoHashKeyValue = new AttributeValue().withN(String.valueOf(geoHashKey));
            }
            attributeValueMap.put(config.getGeoHashKeyColumn(), geoHashKeyValue);
        }
    }

    /**
     * For use with newer DynamoDB
     * @param item
     * @param latitude
     * @param longitude
     * @param configs
     */
    public void updateAttributeValuesForItem(Item item, double latitude, double longitude,
                                      List<GeoConfig> configs) {
        if (configs == null) {
            throw new IllegalArgumentException("Geo configs should not be null");
        }
        for (GeoConfig config : configs) {
            //Fail-fast if any of the preconditions fail
            checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(),
                    config.getGeoHashKeyLength());

            long geohash = s2Manager.generateGeohash(latitude, longitude);
            long geoHashKey = s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength());

            //Decorate the request with the geohash
//            AttributeValue geoHashValue = new AttributeValue().withN(Long.toString(geohash));
//            item.put(config.getGeoHashColumn(), geoHashValue);
            item.withNumber(config.getGeoHashColumn(), geohash);

            AttributeValue geoHashKeyValue;
            if (config.getHashKeyDecorator().isPresent() && config.getCompositeHashKeyColumn().isPresent()) {
//                AttributeValue compositeHashKeyValue = item.get(config.getCompositeHashKeyColumn().get());
                String compositeHashKeyValue = item.getString(config.getCompositeHashKeyColumn().get());
                if (compositeHashKeyValue == null) {
                    continue;
                }
//                String compositeColumnValue = compositeHashKeyValue;
                String hashKey = config.getHashKeyDecorator().get().decorate(compositeHashKeyValue, geoHashKey);
                //Decorate the request with the composite geoHashKey (type String)
//                geoHashKeyValue = new AttributeValue().withS(String.valueOf(hashKey));

                item.withString(config.getGeoHashKeyColumn(), hashKey);
            } else {
                //Decorate the request with the geoHashKey (type Number)
//                geoHashKeyValue = new AttributeValue().withN(String.valueOf(geoHashKey));
                item.withNumber(config.getGeoHashKeyColumn(), geoHashKey);
            }

//            item.put(config.getGeoHashKeyColumn(), geoHashKeyValue);
        }
    }

    /**
     * Decorates the given query request with attributes required for geo spatial querying.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param latitude     the latitude of the item that is being queried
     * @param longitude    the longitude of the item that is being queried
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the decorated request
     */
    public QueryRequest getItemQuery(QueryRequest queryRequest, double latitude, double longitude, GeoConfig config,
                                     Optional<String> compositeKeyValue) {
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength());

        //Generate the geohash and geoHashKey to query by global secondary index
        long geohash = s2Manager.generateGeohash(latitude, longitude);
        long geoHashKey = s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength());
        queryRequest.withIndexName(config.getGeoIndexName());
        Map<String, Condition> keyConditions = new HashMap<String, Condition>();

        //Construct the hashKey condition
        Condition geoHashKeyCondition;
        if (config.getHashKeyDecorator().isPresent() && compositeKeyValue.isPresent()) {
            String hashKey = config.getHashKeyDecorator().get().decorate(compositeKeyValue.get(), geoHashKey);
            geoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                    .withAttributeValueList(new AttributeValue().withS(hashKey));
        } else {
            geoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                    .withAttributeValueList(new AttributeValue().withN(String.valueOf(geoHashKey)));
        }
        keyConditions.put(config.getGeoHashKeyColumn(), geoHashKeyCondition);

        //Construct the geohash condition
        Condition geoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohash)));
        keyConditions.put(config.getGeoHashColumn(), geoHashCondition);

        queryRequest.setKeyConditions(keyConditions);
        return queryRequest;
    }

    /**
     * Decorates the given query request with attributes required for geo spatial querying.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param latitude         the latitude of the item that is being queried
     * @param longitude        the longitude of the item that is being queried
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the decorated request
     */
    public QueryRequest getItemQuery(QueryRequest queryRequest, double latitude, double longitude, String geoIndexName,
                                     String geoHashKeyColumn, String geoHashColumn,
                                     int geoHashKeyLength, Optional<String> compositeKeyValue) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).build();
        return getItemQuery(queryRequest, latitude, longitude, config, compositeKeyValue);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of the radius query.
     * It also contains a filter {@link com.dashlabs.dash.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the radius.
     * This is needed because queries are fired for every cell that intersects with the radius' rectangle box.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param latitude     the latitude of the center point for the radius query
     * @param longitude    the longitude of the center point for the radius query
     * @param radius       the radius (in metres)
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest radiusQuery(QueryRequest queryRequest, double latitude, double longitude, double radius, GeoConfig config, Optional<String> compositeKeyValue) {
        checkArgument(radius >= 0.0d, "radius has to be a positive value: %s", radius);
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength());
        //Center latLong is needed for the radius filter
        S2LatLng centerLatLng = S2LatLng.fromDegrees(latitude, longitude);
        GeoFilter<Map<String, AttributeValue>> filter = GeoFilters.newRadiusFilter(centerLatLng, radius);
        //Bounding box is needed to generate queries for each cell that intersects with the bounding box
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRadiusQuery(latitude, longitude, radius);
        List<QueryRequest> geoQueries = geoQueryHelper.generateGeoQueries(queryRequest, boundingBox, config, compositeKeyValue);
        return new GeoQueryRequest(geoQueries, filter);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of the radius query.
     * It also contains a filter {@link com.dashlabs.dash.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the radius.
     * This is needed because queries are fired for every cell that intersects with the radius' rectangle box.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param latitude         the latitude of the center point for the radius query
     * @param longitude        the longitude of the center point for the radius query
     * @param radius           the radius (in metres)
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest radiusQuery(QueryRequest queryRequest, double latitude, double longitude, double radius, String geoIndexName,
                                       String geoHashKeyColumn, String geoHashColumn,
                                       int geoHashKeyLength, Optional<String> compositeKeyValue) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).build();
        return radiusQuery(queryRequest, latitude, longitude, radius, config, compositeKeyValue);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of this rectangle query.
     * It also contains a filter {@link com.dashlabs.dash.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the bounding box of the queried rectangle.
     * This is needed because queries are fired for every cell that intersects with the rectangle's bounding box.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param minLatitude  the latitude of the min point of the rectangle
     * @param minLongitude the longitude of the min point of the rectangle
     * @param maxLatitude  the latitude of the max point of the rectangle
     * @param maxLongitude the longitude of the max point of the rectangle
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest rectangleQuery(QueryRequest queryRequest, double minLatitude, double minLongitude, double maxLatitude,
                                          double maxLongitude, GeoConfig config, Optional<String> compositeKeyValue) {
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength());
        // bounding box is needed for the filter and to generate the queries
        // for each cell that intersects with the bounding box
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRectangleQuery(minLatitude, minLongitude, maxLatitude, maxLongitude);
        GeoFilter<Map<String, AttributeValue>> filter = GeoFilters.newRectangleFilter(boundingBox);
        List<QueryRequest> geoQueries = geoQueryHelper.generateGeoQueries(queryRequest, boundingBox, config, compositeKeyValue);
        return new GeoQueryRequest(geoQueries, filter);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of this rectangle query.
     * It also contains a filter {@link com.dashlabs.dash.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the bounding box of the queried rectangle.
     * This is needed because queries are fired for every cell that intersects with the rectangle's bounding box.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param minLatitude      the latitude of the min point of the rectangle
     * @param minLongitude     the longitude of the min point of the rectangle
     * @param maxLatitude      the latitude of the max point of the rectangle
     * @param maxLongitude     the longitude of the max point of the rectangle
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest rectangleQuery(QueryRequest queryRequest, double minLatitude, double minLongitude, double maxLatitude,
                                          double maxLongitude, String geoIndexName, String geoHashKeyColumn,
                                          String geoHashColumn, int geoHashKeyLength, Optional<String> compositeKeyValue) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).build();
        return rectangleQuery(queryRequest, minLatitude, minLongitude, maxLatitude, maxLongitude, config, compositeKeyValue);
    }

    /**
     * Checks the values of the geo config
     *
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     */
    private void checkConfigParams(String geoIndexName, String geoHashKeyColumn, String geoHashColumn, int geoHashKeyLength) {
        checkArgument((geoIndexName != null && geoIndexName.length() > 0), "geoIndexName cannot be empty: %s", geoIndexName);
        checkArgument((geoHashKeyColumn != null && geoHashKeyColumn.length() > 0), "geoHashKeyColumn cannot be empty: %s",
                geoHashKeyColumn);
        checkArgument((geoHashColumn != null && geoHashColumn.length() > 0), "geoHashColumn cannot be empty: %s", geoHashColumn);
        checkArgument(geoHashKeyLength > 0, "geoHashKeyLength must be a positive number: %s", String.valueOf(geoHashKeyLength));
    }
}
