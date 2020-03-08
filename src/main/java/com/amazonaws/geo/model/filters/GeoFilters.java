package com.amazonaws.geo.model.filters;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.dashlabs.dash.geo.model.filters.GeoDataExtractor;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import com.dashlabs.dash.geo.model.filters.RadiusGeoFilter;
import com.dashlabs.dash.geo.model.filters.RectangleGeoFilter;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.Map;
import java.util.Optional;

/**
 * Created by mpuri on 3/26/14.
 * Factory methods for {@link GeoFilter}.
 */
public class GeoFilters {

    private static final GeoDataExtractor<Map<String, AttributeValue>> EXTRACTOR = new GeoDataExtractor<Map<String, AttributeValue>>() {
        @Override public Optional<Double> extractLatitude(Map<String, AttributeValue> item) {
            if ((item.get(GeoFilter.LATITUDE_FIELD) != null) && (item.get(GeoFilter.LATITUDE_FIELD).getN() != null)) {
                return Optional.of(Double.valueOf(item.get(GeoFilter.LATITUDE_FIELD).getN()));
            }
            return Optional.empty();
        }

        @Override public Optional<Double> extractLongitude(Map<String, AttributeValue> item) {
            if ((item.get(GeoFilter.LONGITUDE_FIELD) != null) && (item.get(GeoFilter.LONGITUDE_FIELD).getN() != null)) {
                return Optional.of(Double.valueOf(item.get(GeoFilter.LONGITUDE_FIELD).getN()));
            }
            return Optional.empty();
        }
    };

    private static final GeoDataExtractor<Item> EXTRACTOR_V2 = new GeoDataExtractor<Item>() {
        @Override public Optional<Double> extractLatitude(Item item) {
            if ((item.get(GeoFilter.LATITUDE_FIELD) != null) && (item.getNumber(GeoFilter.LATITUDE_FIELD) != null)) {
                return Optional.of(item.getNumber(GeoFilter.LATITUDE_FIELD).doubleValue());
            }
            return Optional.empty();
        }

        @Override public Optional<Double> extractLongitude(Item item) {
            if ((item.get(GeoFilter.LONGITUDE_FIELD) != null) && (item.getNumber(GeoFilter.LONGITUDE_FIELD) != null)) {
                return Optional.of(item.getNumber(GeoFilter.LONGITUDE_FIELD).doubleValue());
            }
            return Optional.empty();
        }
    };

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param centerLatLng      the lat/long of the center of the filter's radius
     * @param radiusInMeter     the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static GeoFilter<Map<String, AttributeValue>> newRadiusFilter(S2LatLng centerLatLng, double radiusInMeter) {
        return com.dashlabs.dash.geo.model.filters.GeoFilters.newRadiusFilter(EXTRACTOR, centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param centerLatLng      the lat/long of the center of the filter's radius
     * @param radiusInMeter     the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static GeoFilter<Item> newRadiusFilterV2(S2LatLng centerLatLng, double radiusInMeter) {
        return com.dashlabs.dash.geo.model.filters.GeoFilters.newRadiusFilterV2(EXTRACTOR_V2, centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by rectangle queries
     *
     * @param latLngRect the bounding box for the filter
     * @return a new instance of the {@link RectangleGeoFilter}
     */
    public static GeoFilter<Map<String, AttributeValue>> newRectangleFilter(S2LatLngRect latLngRect) {
        return com.dashlabs.dash.geo.model.filters.GeoFilters.newRectangleFilter(EXTRACTOR, latLngRect);
    }

}
