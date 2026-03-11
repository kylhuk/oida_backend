# Expanded OSINT Source Registry
## Scope
This registry is designed for global public-OSINT collection and normalization. It combines:
1. **Concrete sources** (APIs, portals, dumps, feeds, archives)
2. **Platform classes and standards** (for discovering the long tail of national, regional, and city portals)
3. **Source families** that repeat at admin0/admin1/admin2/admin3/admin4 levels

It is intentionally broader than a static API list. A truly global crawler needs both named sources and discovery fingerprints to surface country and municipal long-tail data.

Current edition includes **222 concrete sources** across **15 categories**, plus **18 recurring national/subnational source families**.

## Tag legend
- `catalog` — dataset/API registry or discovery layer
- `api` — queryable endpoint
- `bulk` — downloadable files/dumps
- `stream` — realtime feed or websocket
- `archive` — historical corpus or snapshots
- `geospatial` — points, shapes, tiles, maps
- `events` — incident or event records
- `telemetry` — tracks, positions, state vectors
- `entity` — people, companies, vehicles, assets
- `documents` — filings, laws, reports, notices
- `official` — government/intergovernmental
- `community` — community/crowdsourced
- `commercial` — commercial/public API or portal
- `registration` — account or approval usually required
- `noncommercial` — non-commercial or research-use limitation
- `global` — worldwide or multi-country coverage
- `regional` — continental or cross-country regional coverage
- `national` — country-level coverage
- `subnational` — state/province/city/district coverage

## Platform fingerprints to probe during crawling
| Platform / standard | Probe patterns |
|---|---|
| CKAN | /api/3/action/package_search ; /api/3/action/package_show ; dataset pages with /dataset/ |
| Socrata | /api/views ; /resource/<id>.json ; CSV/JSON download links |
| ArcGIS | /arcgis/rest/services ; /FeatureServer ; /MapServer ; /ImageServer |
| ArcGIS Hub | /api/search/v1 ; hub site catalogs and datasets |
| Opendatasoft | /api/explore/v2.1/catalog/datasets ; dataset pages with records/explore |
| GeoNetwork | /geonetwork/srv/api/records ; CSW ; metadata XML |
| GeoNode | /api/v2/ ; /catalogue ; /layers ; /documents |
| OGC API - Features | /collections ; /collections/{id}/items |
| OGC API - Records | /collections or /records depending on implementation |
| WFS / WMS | service=WFS or service=WMS query parameters |
| STAC | /collections ; /search ; /stac ; /api/stac |
| Sitemaps | /sitemap.xml ; /sitemap_index.xml |
| Robots | /robots.txt |
| RSS/Atom/GeoRSS | <link rel='alternate' type='application/rss+xml'> or feed.xml/feed.atom |
| Wayback/CDX | archived URL enumeration for dead or changed sites |

## Category index
- [Discovery, catalogs, platform fingerprints, and archives](#discovery-catalogs-platform-fingerprints-and-archives) — 32 sources
- [Place, boundary, geocoding, and geographic normalization](#place-boundary-geocoding-and-geographic-normalization) — 16 sources
- [Global official statistics, economics, and institutional data](#global-official-statistics-economics-and-institutional-data) — 19 sources
- [Regional and continental hubs](#regional-and-continental-hubs) — 8 sources
- [Maritime, ocean, and coastal sources](#maritime-ocean-and-coastal-sources) — 12 sources
- [Aviation, airports, drones, and mobility](#aviation-airports-drones-and-mobility) — 19 sources
- [Space, satellite, and Earth observation](#space-satellite-and-earth-observation) — 12 sources
- [Conflict, humanitarian, displacement, and migration](#conflict-humanitarian-displacement-and-migration) — 14 sources
- [Weather, climate, environment, biodiversity, and energy](#weather-climate-environment-biodiversity-and-energy) — 20 sources
- [Corporate, ownership, sanctions, procurement, legal, and IP](#corporate-ownership-sanctions-procurement-legal-and-ip) — 22 sources
- [Cybersecurity, internet infrastructure, and network telemetry](#cybersecurity-internet-infrastructure-and-network-telemetry) — 16 sources
- [Research, code, media, social, and public conversation](#research-code-media-social-and-public-conversation) — 17 sources
- [Public ledgers and blockchain OSINT](#public-ledgers-and-blockchain-osint) — 6 sources
- [Governance, democracy, and rights](#governance-democracy-and-rights) — 4 sources
- [Public health and epidemiology](#public-health-and-epidemiology) — 5 sources
- [Recurring national and subnational source families](#recurring-national-and-subnational-source-families) — 18 source families

## Discovery, catalogs, platform fingerprints, and archives

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| DataPortals.org | global | directory of open-data portals across countries, regions, and cities | `catalog, discovery, government, global, open` | public web directory | https://dataportals.org/ |
| DataCatalogs.org | global | meta-catalog of open data catalogs | `catalog, discovery, metadata, global, open` | public meta-catalog | https://datacatalogs.org/ |
| ODIN (Open Data Inventory) | global | country-level inventory of openness and coverage of official statistics | `index, official-stats, discovery, global` | country benchmarking | https://odin.opendatawatch.com/ |
| Data.gov Catalog | United States federal | federal dataset metadata and downstream resource links | `catalog, metadata, government, national, official` | public catalog | https://catalog.data.gov/ |
| api.data.gov | United States federal | shared API gateway for U.S. federal public APIs | `api-gateway, government, national, official` | API gateway | https://api.data.gov/ |
| api.gov.uk | United Kingdom public sector | cross-government API catalogue | `catalog, api, government, national, official` | API directory | https://www.api.gov.uk/ |
| data.europa.eu | Europe / EU | EU open-data catalogue and API discovery | `catalog, metadata, api, regional, official` | European data portal | https://data.europa.eu/ |
| DCAT / DCAT-AP | cross-platform standard | interoperable data-catalog metadata model | `standard, catalog, metadata, discovery` | harvest DCAT feeds and RDF | https://www.w3.org/TR/vocab-dcat-3/ |
| CKAN Action API | global platform class | package/resource metadata and record access for CKAN portals | `platform, catalog, api, metadata, global` | common portal fingerprint | https://docs.ckan.org/en/latest/api/ |
| Socrata / SODA | global platform class | tabular records, metadata, filtering, exports | `platform, api, tabular, metadata, global` | common government portal class | https://dev.socrata.com/ |
| ArcGIS Hub | global platform class | open datasets, map layers, organizations, and data portals | `platform, geospatial, catalog, api, global` | often paired with ArcGIS REST services | https://www.esri.com/en-us/arcgis/products/arcgis-hub/overview |
| ArcGIS REST Services | global platform class | feature services, map services, image services, tiles | `platform, geospatial, api, vector, raster, global` | fingerprint /FeatureServer /MapServer /ImageServer | https://developers.arcgis.com/rest/services-reference/enterprise/ |
| Opendatasoft Explore API | global platform class | dataset metadata and record-level API access | `platform, api, tabular, catalog, global` | common city / national portal class | https://help.opendatasoft.com/apis/ods-explore-v2/ |
| GeoNetwork | global platform class | geospatial metadata catalogs and CSW search | `platform, geospatial, catalog, metadata, global` | common SDI / geoportal class | https://geonetwork-opensource.org/ |
| GeoNode | global platform class | geospatial resources, maps, documents, and APIs | `platform, geospatial, catalog, api, global` | common NGO / government geoportal class | https://docs.geonode.org/ |
| OGC CSW | cross-platform standard | searchable metadata records for geospatial resources | `standard, catalog, metadata, geospatial` | common SDI metadata interface | https://www.ogc.org/standards/cat/ |
| OGC API - Features | cross-platform standard | queryable geospatial feature collections | `standard, geospatial, api, vector` | modern replacement / complement for WFS | https://www.ogc.org/standards/ogcapi-features/ |
| OGC API - Records | cross-platform standard | queryable catalog / metadata records | `standard, catalog, metadata, geospatial` | modern discovery interface | https://ogcapi.ogc.org/records/ |
| OGC WFS | cross-platform standard | feature-level geospatial queries and downloads | `standard, geospatial, api, vector` | legacy but extremely common | https://www.ogc.org/standards/wfs/ |
| OGC WMS | cross-platform standard | rendered map images and layer previews | `standard, geospatial, map, raster` | common for map-only layers | https://www.ogc.org/standards/wms/ |
| STAC | cross-platform standard | spatiotemporal asset catalogs for EO / imagery | `standard, imagery, metadata, geospatial` | critical for satellite / aerial discovery | https://stacspec.org/en/about/ |
| Robots Exclusion Protocol | web standard | crawl-allow/disallow rules and hints | `standard, web, crawl-policy, discovery` | must be respected | https://datatracker.ietf.org/doc/html/rfc9309 |
| Sitemaps / Sitemap Index | web standard | URL inventories and update hints | `standard, discovery, url-list, web` | crawl seed source | https://www.sitemaps.org/protocol.html |
| RSS / Atom / GeoRSS | web feed standards | machine-readable update feeds and geotagged feed items | `feed, discovery, documents, geospatial` | use for newsroom / alerts / notices | https://www.rssboard.org/rss-specification |
| Common Crawl | global archive | large-scale open web corpus and monthly crawl snapshots | `archive, web, bulk, global, open` | broad web discovery and replay | https://commoncrawl.org/ |
| GDELT | global media graph | news-derived events, mentions, themes, document search | `media, events, global, near-real-time, open` | fast global monitoring layer | https://www.gdeltproject.org/data.html |
| Wayback Machine / CDX | global archive | historical snapshots and capture index for public web | `archive, historical, web, api` | historical replay | https://archive.org/help/wayback_api.php |
| Registry of Open Data on AWS | global discovery layer | registry of public datasets hosted via AWS resources | `catalog, cloud, discovery, global` | useful for large scientific/EO/public corpora | https://registry.opendata.aws/ |
| Google BigQuery public datasets | global discovery layer | public datasets queryable in BigQuery | `catalog, cloud, analytics, global` | query-in-place public data | https://docs.cloud.google.com/bigquery/public-data |
| Google Cloud public datasets | global discovery layer | public datasets hosted in Cloud Storage / BigQuery | `catalog, cloud, discovery, global` | public cloud dataset registry | https://docs.cloud.google.com/storage/docs/public-datasets |
| Azure Open Datasets | global discovery layer | curated public datasets accessible in Azure | `catalog, cloud, discovery, global` | curated open data in Azure | https://learn.microsoft.com/en-us/azure/open-datasets/dataset-catalog |
| OpenML | global research registry | discoverable datasets, tasks, runs, and ML resources | `catalog, datasets, api, research, global` | less classic OSINT, but useful public dataset discovery | https://docs.openml.org/ |

## Place, boundary, geocoding, and geographic normalization

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| geoBoundaries | global | ADM0-ADM5 administrative boundaries with metadata | `boundaries, geospatial, admin0-admin5, global, open` | core boundary layer for admin rollups | https://www.geoboundaries.org/api.html |
| GeoNames data dumps | global | global place names, alt names, admin codes, postal data | `places, gazetteer, admin, global, open` | daily downloadable dumps | https://www.geonames.org/export/ |
| GeoNames web services | global | search, reverse geocoding, postal codes, nearby places | `places, geocoding, api, global` | REST/JSON services | https://www.geonames.org/export/web-services.html |
| OpenStreetMap Overpass API | global | queryable OSM features, POIs, networks, tags | `geospatial, vector, poi, global, community` | read-only query layer over OSM | https://wiki.openstreetmap.org/wiki/Overpass_API |
| Nominatim | global | geocoding and reverse geocoding over OSM-derived data | `geocoding, places, addresses, global, community` | public usage policy applies | https://nominatim.org/release-docs/latest/ |
| OpenAddresses | global | open address points and address datasets | `addresses, geospatial, global, open` | bulk address coverage | https://openaddresses.io/ |
| NGA GEOnet Names Server | global outside U.S. | official foreign geographic names and variants | `gazetteer, places, official-names, global` | government geographic names reference | https://geonames.nga.mil/ |
| Marine Regions | global maritime | marine place names, EEZs, maritime boundaries, gazetteer | `maritime, boundaries, gazetteer, geospatial, global` | marine admin / zone normalization | https://www.marineregions.org/ |
| Marine Regions web services | global maritime | WMS/WFS/CSW/REST access to marine boundaries and gazetteer | `maritime, boundaries, geospatial, api, global` | direct service access | https://www.marineregions.org/webservices.php |
| Wikidata Query Service | global | linked entities, identifiers, places, organization relations via SPARQL | `entity, linked-data, places, graph, global, open` | great for enrichment and ID linkage | https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/Wikidata_Query_Help |
| Wikimedia Dumps | global | bulk snapshots of wiki content and metadata | `knowledge, documents, bulk, global, open` | useful for entity and article enrichment | https://dumps.wikimedia.org/ |
| Overture Maps | global | open addresses, buildings, divisions, places, transportation | `geospatial, vector, addresses, buildings, global, open` | good complementary base layer | https://docs.overturemaps.org/ |
| OpenSeaMap | global maritime | nautical / seamark layers derived from open marine mapping | `maritime, geospatial, map, community, global` | useful for ports and seamarks | https://wiki.openstreetmap.org/wiki/OpenSeaMap |
| Mapillary API | global | street-level imagery, sequences, map features, detections | `street-level, imagery, geospatial, api, global` | valuable for local visual OSINT | https://www.mapillary.com/developer/api-documentation |
| KartaView | global | crowdsourced street-view imagery and metadata | `street-level, imagery, geospatial, community, global` | community street-level source | https://kartaview.org/doc/authentication |
| OpenCelliD | global | cell tower geolocation data and region stats | `telecom, geospatial, cell-towers, api, global` | public/community cellular infrastructure OSINT | https://www.opencellid.org/ |

## Global official statistics, economics, and institutional data

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| UNSD API Catalogue | global | catalog of UN statistical APIs and data services | `official-stats, catalog, api, global, official` | high-value umbrella discovery source | https://unstats.un.org/unsd/api/ |
| UNdata | global | official UN datasets across demographic, social, and economic themes | `official-stats, datasets, global, official` | broad UN statistical portal | https://data.un.org/ |
| UN SDG API | global | SDG indicators and metadata | `official-stats, sdg, api, global, official` | programmatic SDG access | https://unstats.un.org/sdgapi/swagger/ |
| World Bank Indicators API | global | development indicators and long time series | `official-stats, economics, time-series, api, global, official` | core country-level enrichment source | https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation |
| World Bank Data360 API | global | metadata and data files for World Bank Data360 datasets | `official-stats, metadata, api, global, official` | complements Indicators API | https://data360.worldbank.org/en/api |
| IMF Data APIs | global | SDMX data and metadata from IMF datasets | `official-stats, economics, api, global, official` | useful for macro-finance and balance data | https://data.imf.org/en/Resource-Pages/IMF-API |
| ECB Data Portal API | Europe / Euro area | ECB statistical data and metadata via SDMX REST | `official-stats, economics, api, regional, official` | good for rates, balances, euro-area stats | https://data.ecb.europa.eu/help/api/overview |
| FRED API | global macro / U.S.-centric | economic data series, releases, categories, vintages | `economics, time-series, api, official-ish` | excellent macro enrichment layer | https://fred.stlouisfed.org/docs/api/fred/ |
| WHO Global Health Observatory | global | health indicators and health statistics | `health, indicators, api, global, official` | public health baseline data | https://www.who.int/data/gho |
| OECD Data APIs | OECD + partners | socioeconomic datasets and API access | `official-stats, economics, api, regional, official` | high-quality structured data | https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html |
| FAOSTAT | global | food, agriculture, production, land, and trade statistics | `agriculture, official-stats, global, official` | core agri-food OSINT layer | https://www.fao.org/faostat/en/ |
| UN Comtrade | global | commodity trade statistics and metadata | `trade, official-stats, api, global, official` | core import/export layer | https://comtradedeveloper.un.org/ |
| IATI API Gateway | global aid transparency | public APIs for aid / development activity data | `aid, finance, api, global, open-standard` | programmatic aid / donor data | https://developer.iatistandard.org/ |
| IATI Datastore | global aid transparency | searchable IATI activity data in XML/CSV/XLSX/JSON | `aid, finance, datastore, global` | bulk and query access | https://docs.datastore.iatistandard.org/ |
| UNECE Data Portal | UNECE region | regional official statistics | `official-stats, regional, api, official` | Europe/Eurasia regional data | https://unece.org/data |
| European Environment Agency Datahub | Europe | environmental datasets, metadata, and services | `environment, geospatial, api, regional, official` | EU environment layer | https://www.eea.europa.eu/en/datahub |
| NASA Open Data Portal | global | NASA public dataset metadata across science and EO | `catalog, science, metadata, global, official` | useful for EO/science discovery | https://data.nasa.gov/ |
| WIPO IP Statistics | global | global intellectual property statistics and downloads | `ip, official-stats, datasets, global, official` | IP trend enrichment | https://www.wipo.int/en/web/ip-statistics |
| IEA API / data services | global energy | energy datasets and API access | `energy, official-stats, api, global` | API key required for some services | https://www.iea.org/documentation |

## Regional and continental hubs

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| openAFRICA | Africa | open datasets from African institutions and organizations | `catalog, datasets, regional, Africa, open` | regional portal family | https://open.africa/ |
| Digital Earth Africa | Africa | continental EO data products and specifications | `imagery, EO, geospatial, regional, Africa` | EO and analysis-ready data | https://docs.digitalearthafrica.org/en/latest/data_specs/index.html |
| Open Data for Africa / AfDB | Africa | African development and socioeconomic datasets | `official-stats, catalog, regional, Africa, official` | AfDB-backed data portal | https://dataportal.opendataforafrica.org/ |
| CEPALSTAT | Latin America & Caribbean | regional socioeconomic indicators and open data | `official-stats, regional, Latin-America, api, official` | ECLAC data layer | https://statistics.cepal.org/portal/cepalstat/open-data.html?lang=en |
| Arab Development Portal | Arab region | regional development datasets and APIs | `official-stats, regional, Arab, api, official` | ESCWA / Arab region data | https://data.unescwa.org/ |
| Pacific Data Hub | Pacific | regional data catalog and API access | `official-stats, regional, Pacific, api, official` | Pacific regional data hub | https://pacificdata.org/ |
| ASEANStats | ASEAN | regional indicators and dashboards | `official-stats, regional, ASEAN, official` | ASEAN regional statistics | https://data.aseanstats.org/ |
| INSPIRE Geoportal | Europe | European geospatial discovery services and themes | `geospatial, catalog, metadata, regional, official` | core EU spatial discovery layer | https://knowledge-base.inspire.ec.europa.eu/overview/use_en |

## Maritime, ocean, and coastal sources

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| Global Fishing Watch | global maritime | vessel identity, port visits, encounters, loitering, AIS-derived layers | `maritime, vessel, ais, events, global, registration` | high-value public maritime intelligence source | https://globalfishingwatch.org/our-apis/documentation |
| Marine Cadastre / U.S. AIS | United States maritime | historical AIS / vessel traffic datasets | `maritime, ais, historical, national, official` | U.S. coastal and port coverage | https://hub.marinecadastre.gov/pages/vesseltraffic |
| AISHub | global community maritime | aggregated AIS feed in XML/JSON/CSV | `maritime, ais, api, stream, community, registration` | community-based AIS | https://www.aishub.net/api |
| AISStream | global community maritime | websocket AIS stream with spatial subscriptions | `maritime, ais, stream, websocket, global` | realtime AIS feed | https://aisstream.io/documentation |
| Equasis | global maritime | ship and company safety-related information | `maritime, safety, vessel, company, global, registration` | public site, automation requires care | https://www.equasis.org/ |
| IMO GISIS | global maritime | ship/company particulars and public maritime modules | `maritime, entity, security, official, global, registration` | module coverage varies | https://gisis.imo.org/Public/Default.aspx |
| MarineTraffic APIs | global maritime | ship positions, port data, and AIS-derived services | `maritime, ais, api, commercial, global` | commercial but widely used OSINT adjunct | https://www.marinetraffic.com/en/p/api-services |
| EMODnet web services | Europe marine | search, visualize, and download EMODnet marine datasets | `maritime, ocean, geospatial, api, regional, official` | major Europe marine data backbone | https://emodnet.ec.europa.eu/en/emodnet-web-service-documentation |
| Marine Regions downloads | global maritime | EEZs, sea areas, marine-and-land union layers | `maritime, boundaries, bulk, global, open` | useful for maritime spatial joins | https://www.marineregions.org/downloads.php |
| NOAA CO-OPS Data API | United States coastal/ocean | water levels, predictions, currents, meteorological observations | `ocean, hydrology, tides, api, national, official` | station-based coastal monitoring | https://api.tidesandcurrents.noaa.gov/api/prod/ |
| NOAA CO-OPS Metadata API | United States coastal/ocean | CO-OPS station and metadata resources | `ocean, metadata, api, national, official` | pair with Data API | https://api.tidesandcurrents.noaa.gov/mdapi/prod/ |
| NOAA CO-OPS ERDDAP | United States coastal/ocean | ERDDAP/OPeNDAP access to observational/model coastal datasets | `ocean, hydrology, api, opendap, national, official` | convenient subset/download interface | https://opendap.co-ops.nos.noaa.gov/ |

## Aviation, airports, drones, and mobility

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| OpenSky Network | global aviation | state vectors, tracks, flights, and aircraft metadata | `aviation, ads-b, telemetry, api, global, noncommercial` | research-grade public aviation telemetry | https://openskynetwork.github.io/opensky-api/ |
| Airplanes.live | global aviation | live ADS-B/MLAT aircraft data through public APIs | `aviation, ads-b, telemetry, api, global, noncommercial` | useful cross-check feed | https://airplanes.live/api-guide/ |
| ADS-B Exchange | global aviation | unfiltered aircraft tracking APIs and historical/live data products | `aviation, ads-b, telemetry, api, commercial, global` | commercial/public-lite options | https://www.adsbexchange.com/data/ |
| FAA Aircraft Registry | United States aviation | aircraft registration records and downloads | `aviation, registry, entity, national, official` | registration and owner context | https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download |
| FAA Airmen Certification database | United States aviation | releasable airmen / pilot data | `aviation, registry, people, national, official` | pilot/operator context | https://www.faa.gov/licenses_certificates/airmen_certification/releasable_airmen_download |
| FAA Data portal | United States aviation | FAA public datasets and API discovery | `aviation, catalog, data, national, official` | broad aviation discovery layer | https://www.faa.gov/data |
| FAA ADIP | United States aviation | airport data and information management records | `aviation, airport, metadata, national, official` | airport infrastructure metadata | https://adip.faa.gov/ |
| FAA NMS / NOTAM | United States aviation | NOTAM access and operational notices | `aviation, notam, safety, api, national, official` | operational / restriction context | https://nms.aim.faa.gov/ |
| FAA UAS Data Delivery System | United States aviation | drone/UAS related data via ArcGIS portal | `aviation, drone, geospatial, national, official` | UAS-focused feed family | https://udds-faa.opendata.arcgis.com/ |
| AviationWeather API | aviation weather | machine-readable aviation weather products | `aviation, weather, api, official` | METAR/TAF/SIGMET-style context | https://aviationweather.gov/data/api/ |
| OpenAIP Core API | global aviation | aeronautical content including airspaces and airports | `aviation, airspace, airport, api, global` | public API with auth | https://docs.openaip.net/ |
| OpenAIP Tiles API | global aviation | Mapbox vector tiles for aeronautical geometries | `aviation, airspace, tiles, api, global` | map rendering / geospatial access | https://docs.openaip.net/?urls.primaryName=Tiles+API |
| OurAirports | global aviation | airport, region, country, runway CSV dumps | `aviation, airport, bulk, global, community` | nightly CSV dumps | https://ourairports.com/data/ |
| OpenFlights | global aviation | airports, airlines, routes, planes, countries | `aviation, routes, airport, bulk, global, community` | helpful for network context | https://openflights.org/data |
| GTFS | global mobility | scheduled transit stops, routes, trips, timetables | `mobility, transit, schedule, open-standard, global` | common public transit source | https://gtfs.org/documentation/overview/ |
| GTFS Realtime | global mobility | vehicle positions, trip updates, service alerts | `mobility, transit, realtime, telemetry, open-standard, global` | realtime transit layer | https://gtfs.org/documentation/realtime/reference/ |
| Mobility Database | global mobility | catalog of GTFS, GTFS-Realtime, and GBFS feeds | `mobility, catalog, feeds, global, open` | major feed discovery source | https://mobilitydatabase.org/feeds |
| Transitland | global mobility | directory and APIs for transit operators and feeds | `mobility, catalog, api, feeds, global` | cross-feed normalization layer | https://www.transit.land/documentation |
| GBFS | global shared mobility | shared mobility status, station availability, pricing, geofencing | `mobility, bikeshare, realtime, open-standard, global` | pull-based JSON feed standard | https://gbfs.org/documentation/reference/ |

## Space, satellite, and Earth observation

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| CelesTrak | global space | GP/TLE/OMM orbital data and satellite catalog products | `space, orbit, tle, catalog, global, open` | primary open orbit source | https://celestrak.org/NORAD/documentation/gp-data-formats.php |
| Space-Track | global space | space catalog, history, conjunction and related data APIs | `space, orbit, catalog, api, global, registration` | registration and rate limits apply | https://www.space-track.org/documentation |
| SatNOGS DB | global space | satellite and transmitter metadata via REST API | `space, transmitter, metadata, community, global` | crowdsourced but valuable | https://docs.satnogs.org/projects/satnogs-db/en/stable/api.html |
| Copernicus Data Space Ecosystem | global EO / Europe-led | catalog, download, STAC, openEO, processing APIs | `imagery, EO, stac, api, global, official` | major open EO backbone | https://dataspace.copernicus.eu/analyse/apis |
| Copernicus catalogue APIs | global EO / Europe-led | multiple REST protocols for catalog querying | `imagery, EO, catalog, api, global` | catalog access patterns | https://dataspace.copernicus.eu/analyse/apis/catalogue-apis |
| openEO in Copernicus Data Space | global EO / Europe-led | cloud-based EO processing through openEO | `imagery, EO, processing, api, global` | useful for derived products | https://dataspace.copernicus.eu/analyse/apis/openeo-api |
| Sentinel Hub APIs | global EO | raw imagery access, rendered images, statistics, analysis | `imagery, EO, api, processing, global` | high-level EO access layer | https://dataspace.copernicus.eu/analyse/apis/sentinel-hub |
| USGS M2M | global EO via U.S. holdings | programmatic search and acquisition of geospatial products | `imagery, EO, search, api, official` | Landsat and related holdings | https://m2m.cr.usgs.gov/ |
| OpenAerialMap | global imagery | open aerial imagery discovery and metadata | `imagery, aerial, catalog, open, global` | good for local orthophotos | https://openaerialmap.org/ |
| Planet public data | global EO | curated public EO datasets surfaced through Planet tooling | `imagery, EO, catalog, public-data, global` | public data only | https://docs.planet.com/data/public-data/ |
| NASA FIRMS | global fire EO | active fire and thermal anomaly detections | `fire, hotspots, EO, near-real-time, global` | high-value hazard layer | https://firms.modaps.eosdis.nasa.gov/api/ |
| Digital Earth Africa | Africa EO | analysis-ready EO products for Africa | `imagery, EO, analysis-ready, regional, Africa` | also fits regional hub category | https://docs.digitalearthafrica.org/en/latest/data_specs/index.html |

## Conflict, humanitarian, displacement, and migration

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| ACLED | global conflict | conflict, protest, and political violence events | `conflict, protest, events, global, api` | high-value political event dataset | https://acleddata.com/acled-api-documentation |
| UCDP API | global conflict | organized violence datasets and event-level access | `conflict, violence, events, api, global` | academic structured conflict source | https://ucdp.uu.se/apidocs/ |
| UCDP downloads | global conflict | bulk downloads of UCDP datasets and maps | `conflict, bulk, events, global` | API and bulk complement each other | https://ucdp.uu.se/downloads/ |
| CrisisWatch | global conflict | monthly country conflict updates and early warning tracking | `conflict, monitoring, documents, global` | mostly narrative but highly useful | https://www.crisisgroup.org/crisiswatch |
| CrisisWatch database | global conflict | browseable conflict-tracker database | `conflict, database, updates, global` | structured browsing layer | https://www.crisisgroup.org/crisiswatch/database |
| ReliefWeb API | global humanitarian | reports, disasters, jobs, organizations, and references | `humanitarian, documents, disaster, api, global` | large humanitarian archive | https://reliefweb.int/help/api |
| HDX HAPI | global humanitarian | standardized humanitarian indicators across crises | `humanitarian, indicators, api, global` | strong normalization layer | https://hapi.humdata.org/ |
| GDACS | global disaster | global disaster alerts and related event products | `disaster, alerts, events, global` | multi-hazard awareness source | https://www.gdacs.org/default.aspx |
| EM-DAT | global disaster | historical disaster occurrence and impact records | `disaster, historical, impacts, global, registration` | high-value historical disaster DB | https://www.emdat.be/ |
| UNHCR Global Public API | global refugees | refugee, asylum, statelessness, and displacement data | `migration, refugees, api, global, official` | public API for Refugee Data Finder | https://www.unhcr.org/what-we-do/reports-and-publications/data-and-statistics/global-public-api |
| UNHCR Refugee Statistics API | global refugees | long-run forcibly displaced population statistics | `migration, refugees, api, global, official` | programmatic refugee stats | https://api.unhcr.org/docs/refugee-statistics.html |
| UNHCR Operational Data Portal | global emergencies | situation views, maps, and emergency operational documents | `humanitarian, migration, documents, maps, global` | field emergency coordination portal | https://data.unhcr.org/ |
| IOM DTM API | global displacement | internal displacement and mobility/vulnerability data | `migration, displacement, api, global, official` | aggregated DTM sharing layer | https://dtm.iom.int/data-and-analysis/dtm-api |
| IDMC API | global internal displacement | global internal displacement database and API | `displacement, migration, api, global` | long-run internal displacement records | https://www.internal-displacement.org/database/api-documentation/ |

## Weather, climate, environment, biodiversity, and energy

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| National Weather Service API | United States weather | forecasts, alerts, observations, grid and station endpoints | `weather, alerts, observations, api, national, official` | high-value operational weather source | https://www.weather.gov/documentation/services-web-api |
| NOAA Climate Data Online | global historical weather via NOAA holdings | historical weather/climate records and station history | `weather, climate, time-series, api, official` | token required | https://www.ncdc.noaa.gov/cdo-web/webservices/getstarted |
| NCEI Access Data Service | global historical weather via NOAA holdings | dataset-specific access to NCEI data holdings | `weather, climate, api, official` | query-oriented NOAA service | https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation |
| Open-Meteo forecast API | global weather | global weather forecasts from multiple weather models | `weather, api, global, open-source` | free / open-source model blending | https://open-meteo.com/en/docs |
| Open-Meteo historical weather API | global weather | historical weather via reanalysis-based API | `weather, historical, api, global` | good lightweight weather source | https://open-meteo.com/en/docs/historical-weather-api |
| Open-Meteo geocoding API | global places/weather | place search for weather workflows | `weather, geocoding, api, global` | pair with weather APIs | https://open-meteo.com/en/docs/geocoding-api |
| OpenAQ | global air quality | global air-quality measurements and latest sensor readings | `environment, air-quality, api, global, open` | aggregates many public sources | https://docs.openaq.org/ |
| GBIF API | global biodiversity | occurrences, taxonomy, datasets, organizations, downloads | `biodiversity, occurrence, api, global, open` | rich natural-world observation source | https://techdocs.gbif.org/en/openapi/ |
| eBird data products | global biodiversity | bird observations, ranges, trends, abundance products | `biodiversity, observations, datasets, global, community` | data access conditions apply | https://science.ebird.org/en/use-ebird-data/download-ebird-data-products |
| USGS Earthquake Catalog API | global seismic | earthquake event search and realtime feeds | `earthquake, hazards, events, api, official` | realtime + historical seismic layer | https://earthquake.usgs.gov/fdsnws/event/1/ |
| USGS Web Services | United States / global selected hazards | earthquake models/products and GIS/web APIs | `hazards, geospatial, api, official` | many additional hazard services | https://earthquake.usgs.gov/ws/ |
| USGS Water Data APIs | United States hydrology | continuous sensors, daily values, monitoring-location metadata | `hydrology, water, time-series, api, official` | surface and groundwater context | https://api.waterdata.usgs.gov/docs/ |
| Global Forest Watch Data API | global forests/land-use | forest cover, alerts, and custom-geometry queries | `environment, forests, api, global` | major land-use / forest OSINT source | https://data-api.globalforestwatch.org/ |
| Global Forest Watch open data portal | global forests/land-use | discoverable datasets, maps, and open-data products | `environment, forests, catalog, global` | portal complement to API | https://data.globalforestwatch.org/ |
| Copernicus Climate Data Store | global climate | reanalysis and climate datasets with API access | `climate, reanalysis, api, global, official` | CDS API for climate products | https://cds.climate.copernicus.eu/user-guide |
| MET Norway Weather API | global weather | public weather API documentation and data access | `weather, api, official, public` | good additional weather feed | https://api.met.no/weatherapi/documentation |
| Global Energy Monitor | global energy infrastructure | energy infrastructure trackers, reports, and downloads | `energy, infrastructure, datasets, global` | major asset-level energy OSINT source | https://globalenergymonitor.org/ |
| Global Integrated Power Tracker | global power infrastructure | unit-level power stations/facilities with ownership, status, geolocation | `energy, infrastructure, power, global` | asset-level geolocated tracker | https://globalenergymonitor.org/projects/global-integrated-power-tracker/ |
| Global Coal Plant Tracker | global power infrastructure | operating/proposed/retired coal power units worldwide | `energy, infrastructure, coal, global` | asset-level power OSINT | https://globalenergymonitor.org/projects/global-coal-plant-tracker/ |
| OpenAerialMap | global imagery | discoverable open aerial imagery and metadata | `imagery, aerial, catalog, global, open` | local mapping and damage assessment | https://openaerialmap.org/ |

## Corporate, ownership, sanctions, procurement, legal, and IP

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| GLEIF API | global legal entities | LEI reference data and ownership relationships | `corporate, entity, ownership, api, global, official` | who-is-who and who-owns-whom | https://www.gleif.org/en/lei-data/gleif-api |
| OpenCorporates API | global companies | company records, officers, filings, jurisdiction data | `corporate, entity, registry, api, global` | broad public-registry aggregator | https://api.opencorporates.com/documentation/API-Reference |
| Open Ownership / BODS | global beneficial ownership | open standard for beneficial ownership data | `ownership, beneficial-ownership, standard, entity, open` | important publication/interchange standard | https://standard.openownership.org/ |
| OFAC Sanctions List Service | United States sanctions | up-to-date sanctions list data and downloads | `sanctions, compliance, watchlist, api, official, national` | primary OFAC distribution channel | https://ofac.treasury.gov/sanctions-list-service |
| UK Sanctions List | United Kingdom sanctions | sanctioned persons, entities, and specified ships | `sanctions, watchlist, official, national` | downloadable/searchable list | https://www.gov.uk/government/publications/the-uk-sanctions-list |
| EU Sanctions Dataset | European Union sanctions | consolidated EU financial sanctions dataset | `sanctions, watchlist, official, regional` | machine-readable EU list | https://data.europa.eu/data/datasets/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions?locale=en |
| UN Security Council Consolidated List | global sanctions | UN consolidated sanctions list | `sanctions, watchlist, official, global` | UN sanctions backbone | https://main.un.org/securitycouncil/en/content/un-sc-consolidated-list |
| SECO Sanctions Search | Switzerland sanctions | Swiss sanctions-target search | `sanctions, watchlist, official, national` | important Swiss jurisdiction layer | https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos/sanktionsmassnahmen/suche_sanktionsadressaten.html |
| OpenSanctions API | global compliance | aggregated sanctions, PEP, and entity graph data | `sanctions, pep, entity-resolution, api, global, open` | strong cross-jurisdiction normalization layer | https://api.opensanctions.org/docs |
| TED API | Europe procurement | published procurement notices and bulk retrieval | `procurement, tenders, api, regional, official` | anonymous access for published notices | https://docs.ted.europa.eu/api/latest/index.html |
| Open Contracting Data Standard | global procurement | standard for notices, awards, contracts, and documents | `procurement, standard, contracts, documents, open` | very important data model / crawler target | https://www.open-contracting.org/data-standard/ |
| USAspending API | United States spending | federal spending and award data | `procurement, spending, finance, api, official, national` | public federal spending API | https://api.usaspending.gov/docs/ |
| SAM.gov Data Services | United States procurement/entity | entity, exclusions, responsibility/qualification, assistance listings APIs | `procurement, entity, exclusions, api, official, national` | API keys / system access rules apply | https://sam.gov/data-services/Documentation |
| SAM.gov Entity Information | United States procurement/entity | entity data files and API entry points | `procurement, entity, exclusions, data-files, official, national` | download and API integration options | https://sam.gov/entity-information |
| CourtListener API | United States legal | case law, dockets, judges, oral-argument audio, filings | `legal, case-law, api, bulk, national` | major public legal OSINT source | https://www.courtlistener.com/help/api/ |
| Open States API | United States state legislatures | legislative bills, legislators, sessions, jurisdictions | `legal, legislation, api, national, open` | state-level legislative OSINT | https://docs.openstates.org/api-v3/ |
| Open States bulk data | United States state legislatures | bulk legislative data exports | `legal, legislation, bulk, national, open` | public domain dedication on most data | https://openstates.org/downloads/ |
| SEC EDGAR APIs | United States securities filings | company submissions and extracted XBRL data | `corporate, filings, xbrl, api, official, national` | core corporate disclosure source | https://www.sec.gov/search-filings/edgar-application-programming-interfaces |
| data.sec.gov APIs | United States securities filings | public JSON EDGAR data APIs | `corporate, filings, api, official, national` | direct API endpoint family | https://data.sec.gov/ |
| WIPO PATENTSCOPE | global patents | international patent search and documents | `patents, IP, search, documents, global, official` | searchable PCT and national collections | https://www.wipo.int/en/web/patentscope |
| WIPO PATENTSCOPE data services | global patents | programmatic access and batch document retrieval options | `patents, IP, api, documents, global, official` | API / SOAP and data products | https://www.wipo.int/en/web/patentscope/data/index |
| WIPO IP API Catalog | global IP | discoverable APIs from IP institutions | `IP, api-catalog, discovery, global, official` | good IP data discovery layer | https://www.wipo.int/en/web/standards/ip-api-catalog/index |

## Cybersecurity, internet infrastructure, and network telemetry

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| CISA KEV | global cyber use | catalog of vulnerabilities known to be exploited in the wild | `cyber, vulnerabilities, watchlist, official` | priority vulnerability signal | https://www.cisa.gov/known-exploited-vulnerabilities |
| NVD APIs | global cyber | CVE/CPE/CWE-linked vulnerability data and changes | `cyber, vulnerabilities, api, bulk, official` | U.S. gov vulnerability repository | https://nvd.nist.gov/developers/vulnerabilities |
| NVD data feeds | global cyber | bulk JSON feeds for vulnerabilities and related data | `cyber, vulnerabilities, bulk, official` | good for local mirrors | https://nvd.nist.gov/vuln/data-feeds |
| RIPEstat Data API | internet infrastructure | ASN, prefix, routing, country and related network data | `network, routing, asn, api, global` | single point of access to many datasets | https://stat.ripe.net/docs/02.data-api |
| BGPStream APIs | internet infrastructure | live and historical BGP data access libraries and broker | `network, routing, stream, historical, global` | high-value routing telemetry source | https://bgpstream.caida.org/docs/api |
| BGPStream Broker HTTP API | internet infrastructure | metadata and access to available BGP data from providers | `network, routing, metadata, api, global` | discovery for BGP data coverage | https://bgpstream.caida.org/docs/api/broker |
| PeeringDB | global interconnection | IXPs, facilities, networks, and interconnection metadata | `network, ixp, facility, entity, global` | physical and logical interconnection context | https://docs.peeringdb.com/api_specs/ |
| Shodan API | global exposure scan | searchable internet-exposed services and devices | `cyber, internet-exposure, api, commercial, global` | commercial/public-tier OSINT workhorse | https://developer.shodan.io/api |
| Censys Search API | global exposure scan | host, service, and certificate search | `cyber, internet-exposure, certificates, api, commercial, global` | strong complement to Shodan | https://search.censys.io/api |
| VirusTotal API | global malware/url/file intel | file, URL, domain, and relationship lookups | `cyber, malware, url, api, commercial/public-tier, global` | broad threat-context source | https://docs.virustotal.com/reference/overview |
| AlienVault OTX | global threat intel | community threat indicators and pulses | `cyber, ioc, threat-intel, api, community` | public threat-sharing source | https://otx.alienvault.com/api |
| urlscan.io | global web scans | rendered page analysis, contacted domains/IPs, artifacts | `cyber, web, sandbox, api, public` | useful for phishing and web infrastructure | https://urlscan.io/docs/api/ |
| URLhaus | global malware URLs | malware distribution URLs and API queries | `cyber, malware, url, api, community, open` | high-signal malware URL feed | https://urlhaus.abuse.ch/api/ |
| ThreatFox | global IOCs | malware-associated indicators via API | `cyber, ioc, api, community, open` | indicator lookups and feeds | https://threatfox.abuse.ch/api/ |
| MalwareBazaar | global malware samples | malware sample metadata and downloads | `cyber, malware, samples, api, community, open` | sample-oriented OSINT | https://bazaar.abuse.ch/api/ |
| Cloudflare Radar | global internet telemetry | internet traffic, outages, routing, and threat-related telemetry | `network, internet, traffic, api, global` | macro internet health and events | https://developers.cloudflare.com/radar/ |

## Research, code, media, social, and public conversation

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| OpenAlex | global research | works, authors, institutions, topics, funders, publishers | `research, entity, graph, api, global, open` | very rich scholarly graph | https://developers.openalex.org/ |
| Crossref REST API | global research | publication metadata, funding, licenses, ORCID/ROR, abstracts | `research, metadata, api, global, open` | core scholarly metadata source | https://www.crossref.org/documentation/retrieve-metadata/rest-api/ |
| GitHub REST API | global code | repository, issue, release, workflow, user/org metadata | `code, repositories, api, global, public` | public software intelligence source | https://docs.github.com/en/rest |
| GitLab REST API | global code | projects, groups, issues, MRs, CI metadata | `code, repositories, api, global, public` | GitLab-hosted public project OSINT | https://docs.gitlab.com/api/rest/ |
| GH Archive | global code history | archived GitHub event timeline data | `code, events, archive, global, open` | time-series view of public GitHub | https://www.gharchive.org/ |
| Libraries.io API | global packages | package metadata across open-source ecosystems | `code, packages, metadata, api, global` | dependency intelligence | https://libraries.io/api |
| OpenSSF Scorecard API | global code security | security posture signals for open-source projects | `code, security, metrics, api, open` | supply-chain signal source | https://api.securityscorecards.dev/ |
| Stack Exchange API | global Q&A | public Q&A content, users, tags, site stats | `community, forums, api, global, public` | developer / technical discussion OSINT | https://api.stackexchange.com/docs |
| YouTube Data API | global media | videos, channels, playlists, metadata, search | `media, video, api, global, public` | major public video OSINT source | https://developers.google.com/youtube/v3 |
| Mastodon API | federated social | public timelines, hashtags, accounts, posts | `social, federated, api, public-timeline, global` | instance policies affect access | https://docs.joinmastodon.org/methods/timelines/ |
| Reddit Data API | global community | Reddit data access for approved developers | `social, forums, api, global, approval-required` | terms and approval apply | https://support.reddithelp.com/hc/en-us/articles/14945211791892-Developer-Platform-Accessing-Reddit-Data |
| Bluesky / AT Protocol | federated social | public-conversation protocol records and APIs | `social, federated, protocol, api, public` | open protocol and SDK ecosystem | https://docs.bsky.app/docs/advanced-guides/atproto |
| AT Protocol docs | federated social | protocol design and public JSON record model | `social, protocol, data-network, public` | direct protocol reference | https://atproto.com/docs |
| Telegram Bot API | messaging/public bot ecosystem | HTTP API for bot-driven Telegram integrations | `messaging, bot, api, public-platform` | useful where public bots/channels are relevant | https://core.telegram.org/bots/api |
| Wikimedia Dumps | global knowledge | bulk wiki content and metadata snapshots | `knowledge, documents, bulk, global, open` | use for article histories and entity context | https://dumps.wikimedia.org/ |
| GDELT DOC / media APIs | global media | document search and media-derived event/mention access | `media, news, api, global, open` | already covered in discovery, but direct media use-case | https://www.gdeltproject.org/data.html |
| Mapillary API | global street-level imagery | street-level imagery and detections | `street-level, imagery, api, global` | also fits geospatial category | https://www.mapillary.com/developer/api-documentation |

## Public ledgers and blockchain OSINT

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| Etherscan API | multi-chain EVM | addresses, transactions, contracts, logs, balances, ABIs | `blockchain, ledger, api, evm, public/commercial` | unified API across supported EVM chains | https://docs.etherscan.io/introduction |
| mempool.space REST API | Bitcoin ecosystem | addresses, transactions, blocks, fees, mining, Lightning data | `blockchain, ledger, api, bitcoin, open-source` | strong Bitcoin public-ledger source | https://mempool.space/docs/api/rest |
| mempool.space WebSocket API | Bitcoin ecosystem | realtime blocks, mempool, transactions, addresses | `blockchain, ledger, websocket, bitcoin, realtime` | realtime Bitcoin telemetry | https://mempool.space/docs/api/websocket |
| Blockchair API | multi-chain blockchain | block, tx, address, analytics across multiple chains | `blockchain, ledger, analytics, api, global` | SQL-like filtering/aggregation | https://blockchair.com/api/docs |
| Google Blockchain Analytics / BigQuery public chains | multi-chain public cloud | queryable blockchain datasets in BigQuery | `blockchain, ledger, analytics, cloud, public` | great for large-scale historical analysis | https://docs.cloud.google.com/blockchain-analytics/docs/supported-datasets |
| Bitcoin BigQuery public dataset | Bitcoin public cloud | historical Bitcoin blockchain data in BigQuery | `blockchain, ledger, analytics, cloud, public` | legacy but still useful reference | https://cloud.google.com/blog/topics/public-datasets/bitcoin-in-bigquery-blockchain-analytics-on-public-data |

## Governance, democracy, and rights

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| Freedom House Freedom in the World | global governance | country-level freedom scores, narratives, downloadable data | `governance, rights, datasets, global` | annual comparative dataset | https://freedomhouse.org/report/freedom-world |
| Freedom House countries and territories data | global governance | country scores and downloadable dataset links | `governance, rights, datasets, global` | country-level access point | https://freedomhouse.org/country/scores |
| V-Dem dataset | global governance | democracy ratings and detailed political indicators | `governance, democracy, datasets, global` | deep comparative governance source | https://www.v-dem.net/data/the-v-dem-dataset/ |
| V-Dem dataset archive | global governance | historical V-Dem releases | `governance, democracy, archive, global` | versioned downloads | https://www.v-dem.net/data/dataset-archive/ |

## Public health and epidemiology

| Source | Scope | Produces | Tags | Access / notes | Official docs |
|---|---|---|---|---|---|
| ECDC data, dashboards, and databases | Europe | infectious disease datasets, dashboards, maps, downloads | `health, epidemiology, datasets, regional, official` | EU/EEA disease surveillance resources | https://www.ecdc.europa.eu/en/data-dashboards-and-databases |
| CDC Digital Gateway | United States | CDC API library and platform entry point | `health, api, official, national` | CDC API discovery | https://developer.cdc.gov/ |
| CDC API library | United States | list of CDC APIs | `health, api, national, official` | API directory | https://developer.cdc.gov/apis |
| CDC WONDER API | United States | automated data query web service over CDC WONDER databases | `health, epidemiology, api, official` | valuable mortality and public health access | https://wonder.cdc.gov/wonder/help/wonder-api.html |
| CDC Open Data | United States | CDC open datasets via data portal | `health, datasets, open-data, official` | Socrata-based CDC data portal | https://data.cdc.gov/ |

## Recurring national and subnational source families

These are not single global endpoints. They are the source types you should expect to discover repeatedly at country, province/state, district, and municipality levels.

| Source family | Typical outputs | Tags |
|---|---|---|
| National statistics offices | country-level indicators, census tables, SDMX series | `official-stats, national, subnational` |
| Open-data portals | datasets, APIs, metadata, files | `catalog, national, subnational` |
| Geoportals / SDIs | boundaries, parcels, roads, land use, geospatial services | `geospatial, boundaries, national, subnational` |
| Official gazettes | laws, decrees, notices, procurement notices | `legal, documents, national, subnational` |
| Parliament / council portals | bills, agendas, votes, transcripts | `legislation, documents, political, national, subnational` |
| Court / case-law portals | opinions, dockets, filings | `legal, case-law, national, subnational` |
| Procurement / tenders portals | tenders, awards, contracts, suppliers | `procurement, contracts, national, subnational` |
| Company / BO registries | companies, directors, shareholders, filings | `corporate, ownership, national` |
| Elections portals | results, precincts, candidates, campaign finance | `elections, political, national, subnational` |
| Police / emergency portals | incident logs, alerts, advisories | `safety, incidents, alerts, subnational` |
| Health surveillance portals | cases, outbreaks, facilities, bulletins | `health, surveillance, alerts, national, subnational` |
| Weather / hydrology portals | forecasts, gauges, warnings, water levels | `weather, hydrology, alerts, national, subnational` |
| Transport authority portals | road closures, rail alerts, airport/port notices, transit feeds | `transport, mobility, alerts, national, subnational` |
| Utilities / outages portals | power/water/telecom outages and planned works | `infrastructure, outages, alerts, subnational` |
| Universities and repositories | papers, theses, labs, data repositories | `research, documents, datasets, national, subnational` |
| NGO / CSO portals | assessments, local crisis data, reports | `humanitarian, rights, documents, subnational` |
| Local media | articles, RSS, local incident reporting | `media, feeds, local, subnational` |
| Public social/video/community surfaces | posts, comments, public media | `social, media, community, global` |

## Operational notes
- Treat **DataPortals.org, DCAT/DCAT-AP, CKAN, Socrata, ArcGIS Hub/REST, Opendatasoft, GeoNetwork, GeoNode, OGC APIs, sitemaps, Common Crawl, GDELT, and Wayback/CDX** as the primary discovery stack for the long tail.
- Use **geoBoundaries + GeoNames + OSM/Overpass + OpenAddresses + Marine Regions** as the minimum geographic normalization backbone for admin rollups and maritime zones.
- Many high-value sources are public but **not unrestricted**: some require registration, approval, or have non-commercial/research terms. Preserve per-source policy metadata in your registry and enforce it in crawl orchestration.
- For country coverage, start from boundary and country lists, then enumerate national portals, regional/state portals, municipality portals, statistics offices, procurement portals, legislative portals, courts, health surveillance, weather/hydrology, transport, utilities, universities, NGOs, and local media.
