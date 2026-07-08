package geocmd

import "math"

// geoShapeType enumerates the search area shapes supported by GEOSEARCH.
type geoShapeType int

const (
	shapeCircular geoShapeType = iota
	shapeRectangle
)

// geoShape fully describes a GEOSEARCH query, mirroring Redis's GeoShape.
// All sizes are stored in meters (already multiplied by the unit conversion).
type geoShape struct {
	type_     geoShapeType
	centerLon float64 // search center longitude (degrees)
	centerLat float64 // search center latitude  (degrees)
	radius    float64 // radius  in meters (CIRCULAR)
	width     float64 // width   in meters (RECTANGLE)
	height    float64 // height  in meters (RECTANGLE)
}

// radDeg converts radians to degrees.
func radDeg(rad float64) float64 { return rad * 180.0 / math.Pi }

// latDistanceMeters returns the great-circle distance along a meridian
// between two latitudes (longitudes ignored). This is the simplified
// haversine used by Redis (geohashGetLatDistance): because latitude is
// always in [-pi/2, pi/2], arcsin(sin(x)) == x.
func latDistanceMeters(lat1d, lat2d float64) float64 {
	return EarthRadiusInMeters * math.Abs(degToRad(lat2d)-degToRad(lat1d))
}

// distanceMeters is the haversine great-circle distance (Redis
// geohashGetDistance). When the two longitudes are equal it collapses to
// latDistanceMeters to avoid expensive trig.
func distanceMeters(lon1d, lat1d, lon2d, lat2d float64) float64 {
	lon1r := degToRad(lon1d)
	lon2r := degToRad(lon2d)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0.0 {
		return latDistanceMeters(lat1d, lat2d)
	}
	lat1r := degToRad(lat1d)
	lat2r := degToRad(lat2d)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * EarthRadiusInMeters * math.Asin(math.Sqrt(a))
}

// inRadius reports whether (lon,lat) is within radius meters of the center,
// and returns the great-circle distance.
func inRadius(s *geoShape, lon, lat float64) (bool, float64) {
	d := distanceMeters(s.centerLon, s.centerLat, lon, lat)
	return d <= s.radius, d
}

// inRectangle reports whether (lon,lat) lies inside the axis-aligned box of
// the given width/height (in meters) centered at the search point, using the
// exact Redis geohashGetDistanceIfInRectangle algorithm.
//
// Latitude distance is checked first (cheaper); then longitude distance is
// measured along the parallel that passes through the candidate point.
func inRectangle(s *geoShape, lon, lat float64) (bool, float64) {
	if latDistanceMeters(lat, s.centerLat) > s.height/2 {
		return false, 0
	}
	if distanceMeters(lon, lat, s.centerLon, lat) > s.width/2 {
		return false, 0
	}
	return true, distanceMeters(s.centerLon, s.centerLat, lon, lat)
}

// withinShape dispatches to the appropriate containment test.
func withinShape(s *geoShape, lon, lat float64) (bool, float64) {
	switch s.type_ {
	case shapeCircular:
		return inRadius(s, lon, lat)
	case shapeRectangle:
		return inRectangle(s, lon, lat)
	}
	return false, 0
}
