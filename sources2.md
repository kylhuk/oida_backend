---
title: Security / Defence OSINT Addendum (New-Only Sources)
version: 2026-03-11
status: draft-for-implementation
audience: LLM implementation agent
scope: additive-only
goal: build a backend that produces a security overview for any region in the world using public/free OSINT sources focused on safety, security, defence, military posture, disruption, and strategic infrastructure
---

# 1. Purpose

This document is an **additive registry**. It contains **only sources that were not listed in the earlier broad OSINT registries**.

It is intended to be handed directly to an implementation agent.

Primary product goal:

- Produce a **regional security overview** for any point, polygon, admin area, coastline segment, airspace region, or maritime zone.
- Detect and explain indicators such as:
  - movement of government / military / state-linked aircraft
  - sudden expansion of security-related airspace restrictions
  - maritime hazard / exercise zones that imply missile tests, launch windows, naval activity, or regional escalation
  - orbital changes and satellite posture that may indicate surveillance or mission changes
  - internet shutdowns, censorship, or communications degradation
  - emergency alerts, embassy security alerts, and civil-protection activation
  - strategic infrastructure exposure and proximity to current security events
  - defence procurement and military build-up signals

This document is **not** a generic catalog of all public data. It is a **development-grade source and analytics spec** focused on security/safety/defence signals.

# 2. Inclusion rules

## Included

- public official portals
- public/free APIs
- open-data projects
- open-source monitoring projects
- free registration sources if the data is still publicly obtainable at zero cost

## Excluded

- paid/commercial-only sources
- sources already listed in previous registries
- purely generic open-data catalogs unless they directly support the security overview objective

## Important caveat

Some sources are public and free but are **not open-source software projects**. For implementation, track this explicitly via `license_class`:

- `open_data`
- `open_source_project`
- `public_official`
- `free_registration`
- `terms_review`

# 3. Expected deliverable from the implementation agent

Build a backend that:

1. ingests the sources in this file
2. normalizes them into a common event/entity/zone model
3. geobinds everything to:
   - point
   - polygon
   - country
   - admin1
   - admin2
   - admin3/admin4 where possible
   - maritime zones / NAVAREA / EEZ where applicable
4. computes regional security analytics
5. exposes results via REST only
6. preserves provenance and explainability for every score and alert

# 4. Minimum canonical model

## 4.1 Source record

```yaml
source_record:
  source_id: string
  source_name: string
  category: enum
  scope: enum(global|regional|national|subnational)
  license_class: enum(open_data|open_source_project|public_official|free_registration|terms_review)
  access_mode: enum(api|bulk|feed|web|rss|geojson|csv|pdf|html)
  realtime: boolean
  historical: boolean
  entity_types: [aircraft, vessel, satellite, org, facility, actor, alert, event, zone, network]
  geo_binding_strategy: [point, polygon, place_name, route_segment, maritime_zone, country_only]
  update_cadence: string
  urls:
    homepage: string
    docs: string
    data: string
  tags: [string]
```

## 4.2 Canonical observation

```yaml
observation:
  id: string
  source_id: string
  observed_at: datetime
  published_at: datetime|null
  category: enum
  subtype: string
  title: string|null
  summary: string|null
  confidence: float
  entity_refs: [string]
  geometry:
    type: enum(point|polygon|bbox|route|none)
    coordinates: any
  place_binding:
    continent_id: string|null
    admin0_id: string|null
    admin1_id: string|null
    admin2_id: string|null
    admin3_id: string|null
    admin4_id: string|null
    maritime_zone_id: string|null
  attrs: object
  provenance:
    source_url: string
    fetch_url: string|null
    fetch_time: datetime
```

# 5. Priority order

## P0
Implement first.

- ADSB.lol
- FAA Graphic TFRs
- FAA National Defense Airspace TFR Areas
- FAA National Security UAS Flight Restrictions
- NGA Navigational Warnings
- MARAD MSCI
- UKMTO
- ONI Worldwide Threat to Shipping
- ESA DISCOSweb
- GCAT
- SIPRI Arms Transfers
- OONI
- IODA
- FEMA IPAWS
- WMO Severe Weather Information Centre
- IAEA PRIS
- Open Infrastructure Map

## P1
Implement next.

- ADSBDB
- ADSB.lol Aircraft Data Links
- USCG BNM / LNM / MSIB / SOLAR
- IHO NAVAREA discovery
- UCS Satellite Database
- UNOOSA Register
- SIPRI Military Expenditure
- DSCA / State arms notifications
- NATO exercise pages
- OSCE transparency pages
- RIPE Atlas
- EuRepoC
- UNEP Strata
- UN Peace & Security Data Hub

## P2
Implement after the core signal graph is stable.

- ASAM
- EUROCONTROL EAD Basic / AIS directory / RAD
- ICS Advisory Project
- SMICI
- START CBRN datasets
- NuFAD
- Embassy alert-page crawlers
- CAP alerting-authority discovery
- OSAC baseline country reports

# 6. New-only source registry

## 6.1 Air, government aircraft, and security-relevant airspace

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| AIR_ADSBLOL_API | ADSB.lol API | global | live air traffic, state vectors, aircraft metadata, map-compatible feeds | api | yes | limited | point, route | open/free air-traffic layer for government/state aircraft monitoring | homepage: https://www.adsb.lol/ ; docs: https://www.adsb.lol/docs/ ; api: https://api.adsb.lol/ | `air`,`ads-b`,`telemetry`,`government-aircraft`,`open_source_project`,`global` |
| AIR_ADSBLOL_HISTORY | ADSB.lol Historical Data | global | daily historical aircraft traces in public releases | bulk | no | yes | point, route | baseline building, historical anomaly detection, replay | docs: https://www.adsb.lol/docs/open-data/historical/ | `air`,`ads-b`,`history`,`bulk`,`open_data`,`global` |
| AIR_ADSBLOL_DATALINKS | ADSB.lol Aircraft Data Links | global | ACARS, HFDL, VDL2 collection endpoints and open datasets | feed, bulk | yes | yes | point, route | captures message-layer aviation signals beyond normal ADS-B coverage | docs: https://www.adsb.lol/docs/open-data/aircraft-data-links/ ; repo: https://github.com/orgs/adsblol/repositories | `air`,`acars`,`hfdl`,`vdl2`,`telemetry`,`open_source_project`,`global` |
| AIR_ADSBDB | adsbdb | global | aircraft/operator/route enrichment by hex, reg, callsign | api | yes | yes | entity, route | enrich suspicious/state aircraft with operator and route context | repo: https://github.com/mrjackwills/adsbdb | `air`,`entity`,`enrichment`,`open_source_project`,`global` |
| AIR_FAA_GRAPHIC_TFR | FAA Graphic TFRs | national (US) | active temporary flight restrictions | web | yes | partial | polygon | strong direct signal for security events, VIP movement, special ops, launches | https://tfr.faa.gov/ | `air`,`tfr`,`security`,`official`,`polygon`,`national` |
| AIR_FAA_DEFENSE_TFR | FAA National Defense Airspace TFR Areas | national (US) | long-term security-related TFR boundary dataset | geospatial | yes | yes | polygon | persistent sensitive-site airspace controls | https://ais-faa.opendata.arcgis.com/datasets/national-defense-airspace-tfr-areas/about | `air`,`tfr`,`sensitive-sites`,`geospatial`,`public_official`,`national` |
| AIR_FAA_NSUAS | FAA National Security UAS Flight Restrictions | national (US) | drone-restriction polygons around sensitive sites | geospatial | yes | yes | polygon | critical-site and national-security facility overlay | https://uas-faa.opendata.arcgis.com/datasets/national-security-uas-flight-restrictions- | `air`,`drone`,`security`,`restricted-areas`,`geospatial`,`public_official`,`national` |
| AIR_EUROCONTROL_EAD | EUROCONTROL EAD Basic | Europe | official aeronautical information access, AIP/AIS references | web | no | yes | polygon, route, place | detects official airspace/publication changes relevant to exercises and restrictions | https://www.eurocontrol.int/service/european-ais-database | `air`,`ais`,`aip`,`Europe`,`free_registration`,`official` |
| AIR_EUROCONTROL_AIS_DIR | EUROCONTROL AIS Online Directory | global | directory of AIS/AIM offices and official aviation information sites | web | no | yes | country, org | discovery layer for state AIP/eAIP crawling | https://www.eurocontrol.int/articles/ais-online | `air`,`directory`,`discovery`,`official`,`global` |
| AIR_EUROCONTROL_RAD | EUROCONTROL Route Availability Document | Europe | route-availability constraints and routing restrictions | web | yes | yes | route, polygon | airspace restriction signal for exercises and conflict-driven rerouting | https://www.nm.eurocontrol.int/RAD/ | `air`,`routing`,`constraints`,`Europe`,`official` |

### Implementation notes

- Build a state/government aircraft classifier on top of ADSB.lol + adsbdb:
  - operator keywords
  - military/embassy/state registrations
  - unusual callsigns
  - repeated visits to military or diplomatic airports
- TFR changes should be diffed over time:
  - new TFR
  - expanded TFR
  - unusual duration
  - security/VIP/space-ops subtype
- Treat official AIP/eAIP and RAD changes as **airspace posture changes**, not just aviation metadata.

---

## 6.2 Maritime, naval, launch-hazard, and shipping-threat signals

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| MAR_NGA_NAVWARN | NGA Navigational Warnings | global | navigational hazards, distress, hazardous ops, launch / missile / exercise warnings | web | yes | limited | polygon, maritime_zone | one of the strongest open signals for naval activity and missile-test areas | https://msi.nga.mil/NavWarnings | `maritime`,`warnings`,`hazards`,`exercise-zones`,`public_official`,`global` |
| MAR_NGA_ASAM | NGA Anti-Shipping Activity Messages | global | hostile acts, piracy, attacks, suspicious events affecting shipping | pdf/web | yes | yes | point, region | direct maritime security incident layer | https://msi.nga.mil/ | `maritime`,`hostile-acts`,`piracy`,`security`,`public_official`,`global` |
| MAR_USCG_BNM | USCG Broadcast Notices to Mariners | national (US) | urgent marine safety broadcasts | web | yes | partial | polygon, waterway | short-fuse warning source for immediate restrictions and hazards | https://www.navcen.uscg.gov/marine-safety-information-broadcasts | `maritime`,`urgent`,`warnings`,`public_official`,`national` |
| MAR_USCG_LNM | USCG Local Notices to Mariners | national (US) | weekly district notices and local maritime changes | web | yes | yes | polygon, district | local waterway closures and change detection | https://www.navcen.uscg.gov/local-notices-to-mariners | `maritime`,`local`,`notices`,`public_official`,`national` |
| MAR_USCG_MSIB | USCG Marine Safety Information Bulletins | national (US) | bulletins affecting vessel/facility security and safety | web | yes | yes | point, polygon, region | safety/security bulletins with strong operational value | https://www.navcen.uscg.gov/msib-national | `maritime`,`bulletins`,`security`,`public_official`,`national` |
| MAR_USCG_SOLAR | USCG SOLAR Chart | national/global relevance | launch/reentry hazard zones and related marine chart overlays | geospatial, web | yes | yes | polygon | launch/reentry risk overlay for coastal and ocean regions | https://www.navcen.uscg.gov/chart | `maritime`,`space-launch`,`reentry`,`hazard-areas`,`public_official` |
| MAR_IHO_NAVAREA | IHO Navigation Warnings on the Web | global | NAVAREA map and coordinator links | web | no | yes | maritime_zone | global discovery layer for national / regional navigation-warning feeds | https://iho.int/navigation-warnings-on-the-web | `maritime`,`navarea`,`discovery`,`official`,`global` |
| MAR_MARAD_MSCI | MARAD MSCI | global | U.S. maritime alerts and advisories on security threats | web | yes | yes | region, route, maritime_zone | strong official signal for military operations and shipping threats | https://www.maritime.dot.gov/msci/maritime-security-communications-industry-msci-web-portal | `maritime`,`security`,`advisories`,`public_official`,`global` |
| MAR_MARAD_ALERTS | MARAD U.S. Maritime Alerts/Advisories | global | active geographic maritime threat advisories | web | yes | yes | region, maritime_zone | current threat-state layer for shipping routes | https://www.maritime.dot.gov/msci-advisories | `maritime`,`alerts`,`threats`,`public_official`,`global` |
| MAR_ONI_WTS | ONI Worldwide Threat to Shipping | global | monthly threat summaries for merchant shipping | web, archive | yes | yes | region, maritime_zone | security-risk context by sea region | https://www.oni.navy.mil/ONI-Reports/Shipping-Threat-Reports/Worldwide-Threat-to-Shipping/ | `maritime`,`threat-intel`,`shipping`,`public_official`,`global` |
| MAR_UKMTO | UKMTO | regional but globally relevant | warnings, advisories, incidents, suspicious activity in the VRA | web | yes | yes | point, region, route | high-value Red Sea / Gulf / Indian Ocean maritime security source | https://www.ukmto.org/ ; warnings: https://www.ukmto.org/ukmto-products/warnings ; advisories: https://www.ukmto.org/ukmto-products/advisories | `maritime`,`security`,`incidents`,`warnings`,`public_official`,`regional` |
| MAR_MSCIO | Maritime Security Centre Indian Ocean (MSCIO) | regional | maritime security updates and vessel-registration context | web | yes | yes | region, route | EU-backed Indian Ocean / Red Sea maritime security context | https://www.mscio.eu/ | `maritime`,`security`,`Indian-Ocean`,`public_official`,`regional` |

### Implementation notes

- Treat maritime warnings as **event zones** with:
  - start time
  - end time
  - hazard type
  - confidence
  - likely cause class:
    - missile test
    - launch/reentry
    - naval exercise
    - piracy/hostile act
    - closure/safety
- Generate route-intersection alerts:
  - shipping lane vs active warning zone
  - port vs nearby maritime advisory
  - EEZ vs repeated warning density

---

## 6.3 Space, surveillance satellites, orbit changes, reentry

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| SPACE_DISCOSWEB | ESA DISCOSweb | global | object catalog, launch, fragmentation, reentry, org metadata | api, web | yes | yes | orbit, ground-track, country | core public object-classification and reentry context | https://discosweb.esoc.esa.int/ | `space`,`catalog`,`fragmentation`,`reentry`,`free_registration`,`global` |
| SPACE_GCAT | GCAT | global | general catalog of artificial space objects and launch/object cross-links | web, bulk | no | yes | orbit, country | strong historical classification context, especially for classified launches | https://planet4589.org/space/gcat/ | `space`,`catalog`,`launches`,`operators`,`open_data`,`global` |
| SPACE_UCS_SATDB | UCS Satellite Database | global | operational satellite metadata and mission classification | bulk, web | no | yes | orbit, country | classify military / reconnaissance / civil / dual-use payloads | https://www.ucs.org/resources/satellite-database | `space`,`classification`,`satellites`,`open_data`,`global` |
| SPACE_UNOOSA_REGISTER | UNOOSA Register of Objects Launched into Outer Space | global | official UN launch-object registry and searchable index | web | no | yes | country, launch, object | state responsibility and official registry cross-check | https://www.unoosa.org/oosa/en/spaceobjectregister/index.html | `space`,`official-registry`,`UN`,`launches`,`global` |

### Implementation notes

- Do **not** try to infer “spy satellite” from one source alone.
- Use a weighted classifier combining:
  - mission class from UCS
  - operator / owner
  - launch history from GCAT
  - object class / fragmentation / reentry context from DISCOSweb
  - previous known categorization from earlier orbital sources already in the base registry
- Build an `orbital_posture_change` event when:
  - new launch by known military/intelligence operator
  - object moves into a mission-useful orbit family
  - repeated passes over target region cross threshold
  - fragmentation/reentry affects regional risk

---

## 6.4 Military transparency, procurement, exercises, force-posture context

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| DEF_SIPRI_ARMS | SIPRI Arms Transfers Database | global | major conventional-arms transfers | web | no | yes | country-country | long-run capability build-up and supplier-recipient graph | https://www.sipri.org/databases/armstransfers | `defense`,`arms-transfers`,`historical`,`global`,`open_data` |
| DEF_SIPRI_MILEX | SIPRI Military Expenditure Database | global | country military spending time series | web | no | yes | country | militarization baseline and long-term trend | https://www.sipri.org/databases/milex | `defense`,`military-spending`,`historical`,`global`,`open_data` |
| DEF_UNROCA | UN Register of Conventional Arms | global | reported transfers, holdings, procurement transparency | web | no | yes | country-country | official transparency layer on arms flows | https://disarmament.unoda.org/en/our-work/cross-cutting-issues/military-confidence-building-measures/register-conventional-arms | `defense`,`arms`,`UN`,`official`,`global` |
| DEF_UN_MILEX | UN Military Expenditure | global | annual military expenditure reporting | web | no | yes | country | official spending transparency layer | https://disarmament.unoda.org/en/our-work/cross-cutting-issues/military-confidence-building-measures/military-expenditure | `defense`,`military-spending`,`UN`,`official`,`global` |
| DEF_DSCA_SALES | DSCA Major Arms Sales Library | national source, global effect | U.S. foreign military sales notifications | web, pdf | yes | yes | country-country | near-forward indicator of future capability shifts | https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library | `defense`,`arms-sales`,`FMS`,`public_official` |
| DEF_STATE_ARMS_NOTIFY | U.S. State Arms Sales Notifications | national source, global effect | congressional arms-sales notifications | web | yes | yes | country-country | forward-looking procurement signal | https://www.state.gov/arms-sales-congressional-notifications | `defense`,`arms-sales`,`notifications`,`public_official` |
| DEF_NATO_EXERCISES | NATO Exercises | regional/global relevance | official exercise calendars and descriptions | web | yes | yes | country, region, polygon | distinguish routine allied exercises from escalation | https://www.nato.int/en/what-we-do/deterrence-and-defence/nato-exercises | `defense`,`exercises`,`NATO`,`official` |
| DEF_OSCE_VIENNA | OSCE Vienna Document | Europe | transparency framework for military activities and verification | web | no | yes | country, region | baseline ruleset for military transparency and exercise context | https://www.osce.org/fsc/74528 | `defense`,`transparency`,`Europe`,`official` |
| DEF_OSCE_GEMI | OSCE Global Exchange of Military Information | Europe/OSCE area | annual force-structure and equipment transparency | web | no | yes | country | force-structure baseline by state | https://www.osce.org/fsc/41384 | `defense`,`force-structure`,`official`,`Europe` |
| DEF_UN_PSDATA | UN Peace & Security Data Hub | global | peacekeeping and political mission datasets, including air assets and personnel | api, web | yes | yes | country, mission, point | mission presence, air assets, staffing, fatalities, budgets | https://psdata.un.org/ ; api docs: https://psdata.un.org/howto/api_structure | `security`,`peacekeeping`,`missions`,`api`,`UN`,`global` |

### Implementation notes

- These are **context** sources, not tactical movement sources.
- Use them to build:
  - baseline militarization score
  - procurement shift score
  - exercise context overlay
  - mission presence overlay

---

## 6.5 Cyber, censorship, outages, critical-infrastructure incidents

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| CYB_OONI | OONI | global | censorship and network interference measurements | api, bulk, web | yes | yes | country, ASN, network | direct signal of information control and communications interference | https://ooni.org/ ; data: https://ooni.org/data/ ; explorer: https://explorer.ooni.org/ | `cyber`,`censorship`,`measurements`,`open_data`,`global` |
| CYB_IODA | IODA | global | macroscopic internet outage detection | api, web | yes | yes | country, region, ASN | regional communications shutdown and outage detection | https://ioda.inetintel.cc.gatech.edu/ | `cyber`,`internet-outages`,`api`,`open_source_project`,`global` |
| CYB_RIPE_ATLAS | RIPE Atlas | global | active network measurements and probes | api, bulk, web | yes | yes | point, country, ASN | latency/routing anomaly confirmation and regional measurement | https://atlas.ripe.net/ | `cyber`,`network`,`measurements`,`public_data`,`global` |
| CYB_EUREPOC | EuRepoC | global | open cyber-incident database with static releases | web, bulk | yes | yes | country, org | foreign/security-policy relevant cyber-incident overlay | https://eurepoc.eu/ ; db: https://eurepoc.eu/database/ | `cyber`,`incidents`,`open_access`,`global` |
| CYB_ICS_ADV | ICS Advisory Project | global-ish, OT-focused | cleaned CISA ICS advisory dataset in CSV form | bulk | no | yes | vendor, sector, country | OT/ICS risk signal for critical infrastructure | https://github.com/icsadvprj/ICS-Advisory-Project | `cyber`,`OT`,`ICS`,`critical-infrastructure`,`open_source_project` |
| CYB_SMICI | START SMICI | global | significant multi-domain incidents against critical infrastructure | web, dataset | no | yes | country, sector | cross-domain infrastructure attack history | https://www.start.umd.edu/data-tools/significant-multi-domain-incidents-against-critical-infrastructure-smici | `critical-infrastructure`,`incidents`,`research`,`open_data` |
| CYB_CBRN_PORTAL | START Violent Non-State Actor CBRN Data Portal | global | searchable CBRN event and actor data portal | web | no | yes | country, actor | CBRN threat context and actor/event history | https://cbrn.umd.edu/ | `CBRN`,`terrorism`,`actors`,`events`,`public_portal` |
| CYB_NUFAD | Nuclear Facilities Attack Database (NuFAD) | global historical | attacks, breaches, sabotage at nuclear facilities | web, dataset | no | yes | point, facility, country | historical nuclear-facility threat modeling | https://www.start.umd.edu/nuclear-facilities-attack-database-frequently-asked-questions | `nuclear`,`facility-security`,`historical`,`open_source_info` |

### Implementation notes

- Model cyber signals at three levels:
  1. **population connectivity**
  2. **network routing / infrastructure**
  3. **critical-infrastructure attack / exposure**
- Build a `communications_disruption` composite from:
  - OONI anomalies
  - IODA country/subnational outages
  - RIPE Atlas divergence
- Build a `critical_infra_cyber_pressure` composite from:
  - EuRepoC incident density
  - ICS advisory pressure
  - SMICI historical exposure
  - facility proximity

---

## 6.6 Public alerts, emergency activation, official security advisories

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| ALERT_IPAWS_LIVE | FEMA IPAWS All-Hazards Feed | national (US) | live public alert feed in CAP-compatible form | feed | yes | no | polygon, county, state | strong official civil-protection activation signal | https://www.fema.gov/emergency-managers/practitioners/integrated-public-alert-warning-system/technology-developers/all-hazards-information-feed | `alerts`,`CAP`,`all-hazards`,`public_official`,`national` |
| ALERT_IPAWS_ARCHIVE | OpenFEMA IPAWS Archived Alerts | national (US) | archived IPAWS alerts | api, bulk | no | yes | polygon, county, state | historical alert-frequency baselines | https://catalog.data.gov/dataset/ipaws-archived-alerts | `alerts`,`historical`,`CAP`,`public_official`,`national` |
| ALERT_WMO_SWIC | WMO Severe Weather Information Centre | global | centralized official severe-weather and hazard warnings | web | yes | yes | country, polygon | authoritative multi-country alert layer | https://severeweather.wmo.int/ | `alerts`,`weather`,`official`,`global` |
| ALERT_WMO_ALERT_AUTH | WMO Register of Alerting Authorities | global | country-by-country registry of official alerting authorities | web, rss | no | yes | country, org | best discovery layer for official national CAP/alert feeds | https://alertingauthority.wmo.int/ | `alerts`,`directory`,`official`,`global` |
| ALERT_WMO_WIS2 | WMO WIS2 | global | open standards-based discovery/publish/notify pattern for weather/environment data | web, standard | yes | yes | country, global | discovery mechanism for official warning/data publication | https://community.wmo.int/site/knowledge-hub/programmes-and-initiatives/wmo-information-system-wis/wis2-overview | `alerts`,`weather`,`discovery`,`standards`,`global` |
| ALERT_ALERT_HUB | Alert-Hub | global | open-source CAP aggregation tooling and alert hubs | web, open-source | yes | yes | country, polygon | normalizes CAP feed ingestion across countries | https://www.alert-hub.org/alert-hubs | `alerts`,`CAP`,`open_source_project`,`global` |
| ALERT_EONET | NASA EONET | global | curated natural-event metadata API | api | yes | yes | point, polygon | fast hazard/event overlay for wildfire, storm, volcano, etc. | https://eonet.gsfc.nasa.gov/ | `hazards`,`events`,`api`,`open_source_project`,`global` |
| ALERT_USGS_VOLCANO | USGS Volcano APIs | global-ish / official | monitored volcano status and event access | api | yes | yes | point, polygon | volcanic operational-risk layer | https://volcanoes.usgs.gov/hans-public/api/volcano/ | `volcano`,`hazards`,`api`,`public_official` |
| ALERT_EMSC | EMSC / LastQuake services | global | earthquake events, felt reports, public services | web, api-ish | yes | yes | point | rapid seismic-risk and eyewitness-response overlay | https://m.emsc-csem.org/Earthquake_data/Data_queries.php | `earthquake`,`hazards`,`felt-reports`,`global` |
| ALERT_TRAVEL_RSS | U.S. Travel Advisories RSS | global | country-level safety/security advisories and feed updates | rss, web | yes | yes | country | lightweight official risk change detector | https://travel.state.gov/content/travel/en/rss.html | `security-advisories`,`RSS`,`public_official`,`global` |
| ALERT_OSAC | OSAC Country Security Reports | global | country baseline security reports and analysis | web | yes | yes | country | useful country operating-environment baseline | https://www.osac.gov/Content/Browse/Report?subContentTypes=Country+Security+Report | `security-reports`,`country-risk`,`public_official`,`global` |
| ALERT_EMBASSY_PAGES | Embassy / Consulate Alert Pages | global recurring family | localized security alerts and emergency notices | web, rss | yes | yes | city, region, country | faster and more local than top-level travel advisories | example: https://ch.usembassy.gov/category/alert/ | `security-alerts`,`embassy`,`localized`,`public_official`,`global` |
| ALERT_NAVCEN_GPS | USCG NAVCEN GUIDE / GPS Problem Reporting | national/global relevance | GPS interference reports and service interruption context | web | yes | yes | point, region | GNSS disruption and jamming-like signal layer | https://www.navcen.uscg.gov/guide-tool | `gnss`,`gps-interference`,`public_official` |
| ALERT_UNEP_STRATA | UNEP Strata | global | geospatial climate-security hotspot platform | web | yes | yes | polygon, country | climate/environment stress that converges with insecurity | https://unepstrata.org/ | `climate-security`,`hotspots`,`geospatial`,`public_official`,`global` |

### Implementation notes

- CAP and alert systems should be normalized into:
  - alert type
  - severity
  - certainty
  - urgency
  - effective time
  - expires
  - area polygons
- Embassy alerts and travel advisories should be treated as **human security / operating environment** signals.
- GNSS interference reports should be their own event family:
  - jamming
  - spoofing suspected
  - service interruption
  - testing / planned outage

---

## 6.7 Strategic infrastructure overlays

| source_id | source_name | scope | output | access | realtime | historical | geo_binding | why_it_matters | urls | tags |
|---|---|---|---|---|---:|---:|---|---|---|---|
| INFRA_IAEA_PRIS | IAEA PRIS | global | reactor specifications, operators, outages, production history | web | yes | yes | point, country | authoritative nuclear infrastructure layer | https://pris.iaea.org/pris/home.aspx | `nuclear`,`critical-infrastructure`,`public_official`,`global` |
| INFRA_GNPT | Global Nuclear Power Tracker | global | nuclear-facility and unit-level geolocated tracker | web, bulk | yes | yes | point, country | nuclear infrastructure exposure overlay | https://globalenergymonitor.org/projects/global-nuclear-power-tracker/ | `nuclear`,`facilities`,`open_data`,`global` |
| INFRA_OPENINFRAMAP | Open Infrastructure Map | global | power, telecoms, oil, gas infrastructure map layers | web | yes | yes | point, line | consequence and corridor-exposure overlay | https://openinframap.org/ | `infrastructure`,`power`,`telecom`,`oil-gas`,`open`,`global` |

### Implementation notes

- These are not “threat feeds.”
- Use them as **consequence multipliers** and **exposure overlays**.

# 7. Recurring source families to crawl everywhere

These are source families, not single services.

| family_id | family_name | what_to_crawl | why |
|---|---|---|---|
| FAM_EAIP | State AIP / eAIP / AIP SUP / AIC portals | official aviation publications and supplements | detect airspace changes, exercise notes, temporary procedures, long-duration restrictions |
| FAM_NAVWARN | NAVAREA / NAVTEX / hydrographic office warning pages | national and regional navigational warnings | detect exercise zones, launch hazards, closures, and maritime risk |
| FAM_EMBASSY_ALERTS | embassy / consulate alert archives | local security alerts, demonstrations, evacuation notices | strong localized civil-security signal |
| FAM_CAP | national CAP feeds and alerting-authority pages | official emergency alerts and feed endpoints | country-by-country all-hazards awareness |
| FAM_CIVIL_DEFENSE | national civil protection / emergency management sites | evacuation orders, sirens, public guidance | rapid crisis activation signal |
| FAM_DEF_EXERCISES | ministry of defence / armed forces exercise pages | exercise notices, ranges, closures, maneuvers | force-posture context |
| FAM_HYDROGRAPHIC | hydrographic office notices and mariner pages | coastal safety, closures, chart notices | maritime disruption and warning context |
| FAM_AIRSPACE_RESTRICTIONS | civil aviation authority restriction portals | NOTAM/TFR/restriction notices | state airspace posture changes |

# 8. Security analytics to implement

## 8.1 government_air_activity_score

**Question:** Is there unusual government/state/military-linked air activity in the region?

**Inputs:**
- AIR_ADSBLOL_API
- AIR_ADSBLOL_HISTORY
- AIR_ADSBDB
- AIR_FAA_GRAPHIC_TFR
- AIR_EUROCONTROL_RAD
- FAM_EAIP

**Calculation outline:**
1. classify aircraft as:
   - government
   - military
   - state-linked
   - diplomatic/transport
   - unknown
2. count entries/exits over region and nearby airports
3. compare to 30d / 90d baseline
4. add boosts for:
   - unusual concentration
   - night movement spikes
   - repeated shuttle patterns
   - coincident new TFRs or route restrictions

**Output:**
- `0-100` score
- explainability payload
- top contributing aircraft / routes / airports

---

## 8.2 airspace_security_pressure_score

**Question:** Has official security-related airspace control around the region increased?

**Inputs:**
- AIR_FAA_GRAPHIC_TFR
- AIR_FAA_DEFENSE_TFR
- AIR_FAA_NSUAS
- AIR_EUROCONTROL_EAD
- AIR_EUROCONTROL_RAD
- FAM_EAIP
- FAM_AIRSPACE_RESTRICTIONS

**Calculation outline:**
- new restriction count
- area increase of restricted polygons
- restriction duration anomaly
- special categories:
  - security
  - VIP
  - launch
  - drone restriction
- route/reroute impact within buffer

**Output:**
- `0-100` score
- list of new/expanded restriction zones

---

## 8.3 maritime_security_pressure_score

**Question:** Is maritime tension or naval/security activity increasing near the region?

**Inputs:**
- MAR_NGA_NAVWARN
- MAR_NGA_ASAM
- MAR_USCG_BNM
- MAR_USCG_LNM
- MAR_USCG_MSIB
- MAR_MARAD_MSCI
- MAR_ONI_WTS
- MAR_UKMTO
- FAM_NAVWARN

**Calculation outline:**
- active warning density by area and time
- hostilities / suspicious-activity count
- launch/missile/naval-exercise inferred zones
- trend vs 7d / 30d baseline
- route / port intersection count

**Output:**
- `0-100` score
- active threat polygons
- recent high-signal incidents

---

## 8.4 orbital_posture_change_score

**Question:** Has satellite posture changed in a way that matters for this region?

**Inputs:**
- SPACE_DISCOSWEB
- SPACE_GCAT
- SPACE_UCS_SATDB
- SPACE_UNOOSA_REGISTER
- prior orbital sources from the base registry

**Calculation outline:**
- classify satellites by mission/use
- compute overpass density over region
- detect new launches / reclassifications
- detect sudden increase in relevant overpass opportunity
- track reentries or fragmentation with regional consequence

**Output:**
- `0-100` score
- list of relevant objects
- pass-density trend
- mission-class breakdown

---

## 8.5 defense_procurement_shift_score

**Question:** Is there evidence of capability build-up affecting the region?

**Inputs:**
- DEF_SIPRI_ARMS
- DEF_SIPRI_MILEX
- DEF_UNROCA
- DEF_UN_MILEX
- DEF_DSCA_SALES
- DEF_STATE_ARMS_NOTIFY

**Calculation outline:**
- new arms-transfer announcements
- major spending increases
- supplier concentration changes
- weapon-category relevance to the region
- lag-aware weighting:
  - notification
  - contract
  - reported transfer
  - inventory transparency

**Output:**
- `0-100` score
- country pair links
- categories driving the increase

---

## 8.6 exercise_and_force_posture_context_score

**Question:** Are current military exercises or force-structure changes likely affecting the regional picture?

**Inputs:**
- DEF_NATO_EXERCISES
- DEF_OSCE_VIENNA
- DEF_OSCE_GEMI
- DEF_UN_PSDATA
- FAM_DEF_EXERCISES

**Calculation outline:**
- current exercise density
- proximity of exercises to region
- force-structure baseline and recent public changes
- peacekeeping/security mission footprint

**Output:**
- contextual score and explanation
- “routine activity” vs “elevated posture” hint

---

## 8.7 communications_disruption_score

**Question:** Is the region experiencing communications suppression or abnormal network degradation?

**Inputs:**
- CYB_OONI
- CYB_IODA
- CYB_RIPE_ATLAS
- CYB_EUREPOC

**Calculation outline:**
- country/subnational outage signal
- censorship anomaly count
- probe divergence
- related cyber-incident density

**Output:**
- `0-100` score
- outage polygons / affected networks
- suspected cause class:
  - outage
  - shutdown
  - censorship
  - infrastructure incident
  - unknown

---

## 8.8 civil_protection_activation_score

**Question:** Is the region under abnormal public-alert or civil-protection pressure?

**Inputs:**
- ALERT_IPAWS_LIVE / ARCHIVE
- ALERT_WMO_SWIC
- ALERT_ALERT_HUB
- ALERT_EONET
- ALERT_USGS_VOLCANO
- ALERT_EMSC
- FAM_CAP
- FAM_CIVIL_DEFENSE

**Calculation outline:**
- alert volume vs baseline
- severity-weighted alert density
- multi-source hazard co-occurrence
- persistence and escalation over time

**Output:**
- `0-100` score
- active alert count
- top alert types
- alert polygons / affected population estimates if available

---

## 8.9 human_security_signal_score

**Question:** Are official travel, embassy, or security advisories worsening for this region?

**Inputs:**
- ALERT_TRAVEL_RSS
- ALERT_OSAC
- ALERT_EMBASSY_PAGES

**Calculation outline:**
- advisory level change
- new embassy alerts
- keyword classification:
  - unrest
  - border closure
  - attacks
  - terrorism
  - kidnapping
  - transport disruption
  - evacuation / shelter guidance

**Output:**
- `0-100` score
- advisory delta summary
- country/city alert excerpts and provenance

---

## 8.10 infrastructure_exposure_score

**Question:** If security conditions worsen, how exposed is the region’s strategic infrastructure?

**Inputs:**
- INFRA_IAEA_PRIS
- INFRA_GNPT
- INFRA_OPENINFRAMAP
- CYB_SMICI
- CYB_NUFAD

**Calculation outline:**
- infrastructure density
- criticality weighting
- proximity to active threat / hazard zones
- historical attack exposure
- corridor concentration

**Output:**
- `0-100` score
- top exposed assets
- sector breakdown:
  - nuclear
  - power
  - telecom
  - oil/gas

---

## 8.11 regional_security_overview_score

**Question:** What is the combined security posture of the region right now?

**Suggested weighted composition:**

```text
regional_security_overview_score =
  0.18 * government_air_activity_score +
  0.12 * airspace_security_pressure_score +
  0.18 * maritime_security_pressure_score +
  0.10 * orbital_posture_change_score +
  0.08 * defense_procurement_shift_score +
  0.08 * exercise_and_force_posture_context_score +
  0.12 * communications_disruption_score +
  0.08 * civil_protection_activation_score +
  0.06 * human_security_signal_score +
  0.10 * infrastructure_exposure_score
```

Weights should remain configurable.

# 9. REST API targets

## 9.1 Core endpoints

```text
GET /v1/regions/overview?lat={lat}&lon={lon}&radius_km={r}
GET /v1/regions/overview?place_id={placeId}
GET /v1/regions/overview?admin0={code}
GET /v1/regions/overview?admin1={id}
GET /v1/regions/overview?polygon={wkt|geojson}
```

## 9.2 Domain endpoints

```text
GET /v1/air/activity
GET /v1/air/restrictions
GET /v1/maritime/security
GET /v1/space/posture
GET /v1/defense/procurement
GET /v1/cyber/disruptions
GET /v1/alerts/active
GET /v1/infrastructure/exposure
```

## 9.3 Explainability endpoints

```text
GET /v1/metrics/{metricId}/explain?region={...}
GET /v1/events?region={...}&source_id={...}
GET /v1/entities/{entityId}
GET /v1/zones/active?region={...}
```

# 10. Implementation tasks

## Phase 1 — source registry and connectors

- create a machine-readable source registry from section 6
- implement connectors for all P0 sources
- store raw payloads with full provenance
- create a fetch scheduler with per-source cadence
- add source-health monitoring and change detection

## Phase 2 — normalization

- normalize aircraft, zone, alert, maritime warning, cyber incident, and infrastructure records
- build mappers for:
  - time windows
  - severity
  - event type
  - source confidence
- add geometry extraction and normalization

## Phase 3 — entity and event logic

- aircraft classifier:
  - government / military / state-linked / unknown
- satellite classifier:
  - military / reconnaissance / civil / dual-use / unknown
- warning-zone classifier:
  - launch / reentry / missile / exercise / piracy / safety / unknown
- advisory classifier:
  - unrest / attack / border / terror / evacuation / transport / infrastructure

## Phase 4 — geobinding

- bind every record to:
  - point
  - polygon
  - admin hierarchy
  - maritime zone when applicable
- support buffers around:
  - airports
  - ports
  - military bases if available from other registries
  - infrastructure assets

## Phase 5 — analytics

- implement all metrics in section 8
- store both raw contributions and final scores
- support:
  - country
  - continent
  - world
  - custom polygon
  - rolling time windows

## Phase 6 — REST serving

- expose endpoints in section 9
- paginate event/entity lists
- return score explanations by default
- preserve backward compatibility in response schemas

## Phase 7 — QA and replay

- historical replay over at least 90 days where source coverage allows
- source drift tests
- schema drift tests
- anomaly-quality review reports
- per-source parser fixtures

# 11. Acceptance criteria

The implementation is acceptable when:

- all P0 sources are ingesting successfully
- every observation has provenance
- every geocodable observation is attached to a region
- `/v1/regions/overview` returns:
  - score values
  - component metrics
  - top contributing events
  - top contributing entities
  - active zones
- the system can explain:
  - why a region score rose
  - which sources caused it
  - which events/entities/zones matter most
- historical replay reproduces prior daily scores deterministically

# 12. Final instruction to the implementation agent

Build this addendum as a **strict extension** of the earlier base registry.

Do not re-list old sources in the new source registry. Keep source IDs stable. Add a `source_set = "security_addendum_new_only"` marker to every source defined here.

When uncertain about classification:
- preserve raw evidence
- classify as `unknown`
- emit explainability rather than overconfident labels
- make all weights configurable

End of file.
