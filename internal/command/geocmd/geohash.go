package geocmd

const (
	EarthRadiusInMeters = 6372797.560856

	GeoLatMin = -85.05112878
	GeoLatMax =  85.05112878
	GeoLongMin = -180.0
	GeoLongMax =  180.0
)

type GeoHashBits struct {
	Bits uint64
	Step uint8
}

type GeoHashRange struct {
	Min float64
	Max float64
}

func geohashEncode(lonRange, latRange GeoHashRange, lon, lat float64, step uint8) GeoHashBits {
	var hash GeoHashBits
	hash.Step = step
	
	if lon > lonRange.Max || lon < lonRange.Min || lat > latRange.Max || lat < latRange.Min {
		return hash
	}
	
	latOffset := (lat - latRange.Min) / (latRange.Max - latRange.Min)
	lonOffset := (lon - lonRange.Min) / (lonRange.Max - lonRange.Min)
	
	latOffset *= float64(uint64(1) << step)
	lonOffset *= float64(uint64(1) << step)
	
	var ilat = uint64(latOffset)
	var ilon = uint64(lonOffset)
	
	hash.Bits = interleave64(ilat, ilon)
	return hash
}

func interleave64(xlo, ylo uint64) uint64 {
	x := xlo & 0x0000000007ffffff
	y := ylo & 0x0000000007ffffff

	x = (x | (x << 16)) & 0x0000ffff0000ffff
	y = (y | (y << 16)) & 0x0000ffff0000ffff
	x = (x | (x << 8))  & 0x00ff00ff00ff00ff
	y = (y | (y << 8))  & 0x00ff00ff00ff00ff
	x = (x | (x << 4))  & 0x0f0f0f0f0f0f0f0f
	y = (y | (y << 4))  & 0x0f0f0f0f0f0f0f0f
	x = (x | (x << 2))  & 0x3333333333333333
	y = (y | (y << 2))  & 0x3333333333333333
	x = (x | (x << 1))  & 0x5555555555555555
	y = (y | (y << 1))  & 0x5555555555555555

	return x | (y << 1)
}

func Align52Bits(hash GeoHashBits) uint64 {
	return hash.Bits << (52 - hash.Step*2)
}

func GeoHashEncodeWGS84(lon, lat float64) uint64 {
	lonRange := GeoHashRange{Min: GeoLongMin, Max: GeoLongMax}
	latRange := GeoHashRange{Min: GeoLatMin, Max: GeoLatMax}
	
	hash := geohashEncode(lonRange, latRange, lon, lat, 26)
	return Align52Bits(hash)
}

func ExtractB52(bits uint64) uint64 {
	return bits
}
