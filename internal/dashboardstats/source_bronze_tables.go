package dashboardstats

var generatedSourceBronzeTables = []string{
	"src_seed_gdelt_v1",
	"src_fixture_reliefweb_v1",
	"src_fixture_acled_v1",
	"src_fixture_opensanctions_v1",
	"src_fixture_nasa_firms_v1",
	"src_fixture_noaa_hazards_v1",
	"src_fixture_kev_v1",
}

func sourceBronzeTables() []string {
	items := make([]string, len(generatedSourceBronzeTables))
	copy(items, generatedSourceBronzeTables)
	return items
}
