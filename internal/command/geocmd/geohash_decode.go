package geocmd

import "math"

func geohashDecode(hash GeoHashBits, lonRange, latRange GeoHashRange) (float64, float64) {
	hash.Bits <<= (64 - hash.Step*2)
	
	latRangeMin, latRangeMax := latRange.Min, latRange.Max
	lonRangeMin, lonRangeMax := lonRange.Min, lonRange.Max
	
	for i := uint8(0); i < hash.Step; i++ {
		if (hash.Bits & 0x8000000000000000) != 0 {
			lonRangeMin = (lonRangeMin + lonRangeMax) / 2
		} else {
			lonRangeMax = (lonRangeMin + lonRangeMax) / 2
		}
		hash.Bits <<= 1
		
		if (hash.Bits & 0x8000000000000000) != 0 {
			latRangeMin = (latRangeMin + latRangeMax) / 2
		} else {
			latRangeMax = (latRangeMin + latRangeMax) / 2
		}
		hash.Bits <<= 1
	}
	
	lon := (lonRangeMin + lonRangeMax) / 2
	lat := (latRangeMin + latRangeMax) / 2
	
	return lon, lat
}

func GeoHashDecodeWGS84(bits uint64) (float64, float64) {
	lonRange := GeoHashRange{Min: GeoLongMin, Max: GeoLongMax}
	latRange := GeoHashRange{Min: GeoLatMin, Max: GeoLatMax}
	
	// Un-align from 52 bits to step=26
	hash := GeoHashBits{
		Bits: bits,
		Step: 26,
	}
	
	return geohashDecode(hash, lonRange, latRange)
}

func degToRad(deg float64) float64 {
	return deg * math.Pi / 180.0
}

func GeoDistance(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r := degToRad(lon1)
	lon2r := degToRad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0.0 {
		return latDistanceMeters(lat1, lat2)
	}
	lat1r := degToRad(lat1)
	lat2r := degToRad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * EarthRadiusInMeters * math.Asin(math.Sqrt(a))
}
