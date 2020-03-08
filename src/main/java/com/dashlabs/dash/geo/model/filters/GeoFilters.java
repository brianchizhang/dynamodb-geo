package com.dashlabs.dash.geo.model.filters;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

/**
 *
 * Originally created by mpuri on 3/26/14.
 * Factory methods for {@link GeoFilter}.
 *
 * User: blangel
 * Date: 7/19/17
 * Time: 2:11 PM
 *
 * Modified to abstract data type.
 */
public class GeoFilters {

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param extractor to extract data from an item
     * @param centerLatLng  the lat/long of the center of the filter's radius
     * @param radiusInMeter the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static <T> GeoFilter<T> newRadiusFilter(GeoDataExtractor<T> extractor, S2LatLng centerLatLng, double radiusInMeter) {
        return new RadiusGeoFilter<T>(extractor, centerLatLng, radiusInMeter);
    }

    public static <T> GeoFilter<T> newRadiusFilterV2(GeoDataExtractor<T> extractor, S2LatLng centerLatLng, double radiusInMeter) {
        return new RadiusGeoFilter<T>(extractor, centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by rectangle queries
     *
     * @param latLngRect the bounding box for the filter
     * @return a new instance of the {@link RectangleGeoFilter}
     */
    public static <T> GeoFilter<T> newRectangleFilter(GeoDataExtractor<T> extractor, S2LatLngRect latLngRect) {
        return new RectangleGeoFilter<T>(extractor, latLngRect);
    }

}
