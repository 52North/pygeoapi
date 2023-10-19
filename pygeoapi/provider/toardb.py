# =================================================================
#
# Authors: Jan Speckamp <j.speckamp@52North.org>
#
# Copyright (c) 2023 Jan Speckamp
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
import json
import logging
import numbers
from datetime import timedelta
from requests_cache import CachedSession

from pygeoapi.api import F_JSON, F_GEOJSON, F_SENSORML_JSON, CONFORMANCE
from pygeoapi.provider.base import BaseProvider, ProviderItemNotFoundError
from pygeoapi.provider.connectedsystems import *

LOGGER = logging.getLogger(__name__)


class ToarDBProvider(ConnectedSystemsBaseProvider):
    """generic Tile Provider ABC"""

    BASEURL = "https://toar-data.fz-juelich.de/api/v2/"

    OWL_PREFIX = "https://toar-data.fz-juelich.de/documentation/ontologies/v1.0#OWLClass_000"

    OWL_LOOKUP = {
        "type": "https://toar-data.fz-juelich.de/api/v2/controlled_vocabulary/Station%20Type",
        "mean_topography_srtm_alt_90m_year1994": OWL_PREFIX + "053",
        "mean_topography_srtm_alt_1km_year1994": OWL_PREFIX + "054",
        "max_topography_srtm_relative_alt_5km_year1994": OWL_PREFIX + "055",
        "min_topography_srtm_relative_alt_5km_year1994": OWL_PREFIX + "056",
        "stddev_topography_srtm_relative_alt_5km_year1994": OWL_PREFIX + "057",
        "climatic_zone_year2016": OWL_PREFIX + "058",
        "htap_region_tier1_year2010": OWL_PREFIX + "059",
        "dominant_landcover_year2012": OWL_PREFIX + "060",
        "landcover_description_25km_year2012": OWL_PREFIX + "061",
        "dominant_ecoregion_year2017": OWL_PREFIX + "062",
        "ecoregion_description_25km_year2017": OWL_PREFIX + "063",
        "distance_to_major_road_year2020": OWL_PREFIX + "064",
        "mean_stable_nightlights_1km_year2013": OWL_PREFIX + "065",
        "mean_stable_nightlights_5km_year2013": OWL_PREFIX + "066",
        "max_stable_nightlights_25km_year2013": OWL_PREFIX + "067",
        "max_stable_nightlights_25km_year1992": OWL_PREFIX + "068",
        "mean_population_density_250m_year2015": OWL_PREFIX + "069",
        "mean_population_density_5km_year2015": OWL_PREFIX + "070",
        "max_population_density_25km_year2015": OWL_PREFIX + "071",
        "mean_population_density_250m_year1990": OWL_PREFIX + "072",
        "mean_population_density_5km_year1990": OWL_PREFIX + "073",
        "max_population_density_25km_year1990": OWL_PREFIX + "074",
        "mean_nox_emissions_10km_year2015": OWL_PREFIX + "075",
        "mean_nox_emissions_10km_year2000": "",  # empty because OWLClass_000076 provides _year1990 and not 2000
        "wheat_production_year2000": OWL_PREFIX + "077",
        "rice_production_year2000": OWL_PREFIX + "078",
        "omi_no2_column_years2011to2015": OWL_PREFIX + "079",
        "toar1_category": OWL_PREFIX + "080",
        "station_id": OWL_PREFIX + "634",
        "coordinate_validation_status": OWL_PREFIX + "121",
        "country": OWL_PREFIX + "122",
        "state": OWL_PREFIX + "123",
        "type_of_environment": OWL_PREFIX + "124",
        "type_of_area": OWL_PREFIX + "125",
        "timezone": OWL_PREFIX + "126",
    }

    UOM_LOOKUP = {
        "mean_topography_srtm_alt_90m_year1994": "m",
        "mean_topography_srtm_alt_1km_year1994": "m",
        "max_topography_srtm_relative_alt_5km_year1994": "m",
        "min_topography_srtm_relative_alt_5km_year1994": "m",
        "stddev_topography_srtm_relative_alt_5km_year1994": "m",
        "distance_to_major_road_year2020": "m",
        "mean_stable_nightlights_1km_year2013": "unkown",
        "mean_stable_nightlights_5km_year2013": "unkown",
        "max_stable_nightlights_25km_year2013": "unkown",
        "max_stable_nightlights_25km_year1992": "unkown",
        "mean_population_density_250m_year2015": "residents km-2",
        "mean_population_density_5km_year2015": "residents km-2",
        "max_population_density_25km_year2015": "1/km^2",
        "mean_population_density_250m_year1990": "residents km-2",
        "mean_population_density_5km_year1990": "residents km-2",
        "max_population_density_25km_year1990": "1/km^2",
        "mean_nox_emissions_10km_year2015": "kg m-2 s-1",
        "mean_nox_emissions_10km_year2000": "kg m-2 s-1",
        "wheat_production_year2000": "thousand tons",
        "rice_production_year2000": "thousand tons",
        "omi_no2_column_years2011to2015": "10^15 molecules cm-2",
    }

    META_URL = BASEURL + "stationmeta/"
    TIMESERIES_URL = BASEURL + "timeseries/"
    DATA_URL = BASEURL + "data/timeseries/"

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.toardb.ToarDBProvider
        """

        super().__init__(provider_def)
        self.base_url = provider_def["base_url"]

        self.session = CachedSession(
            'toardb_provider_cache',
            use_cache_dir=True,
            cache_control=False,
            expire_after=timedelta(days=1),
            allowable_codes=[200],
            allowable_methods=['GET'],
            stale_if_error=True
        )

    def get_conformance(self) -> List[str]:
        return [
            "whatever",
        ]

    def get_collections(self) -> Dict[str, Dict]:
        """Returns the list of collections that are served by this provider"""

        return {
            "toardb":
                {
                    "type": "collection",
                    "title": {
                        "en": "TOAR"
                    },
                    "description": {
                        "en": "Tropospheric Ozone Assessment Report (TOAR) data"
                    },
                    "keywords": {
                        "en": [
                            "toar",
                            "atmosphere",
                            "ozone"
                        ]
                    },
                    "links": [
                        {
                            "type": "text/html",
                            "rel": "canonical",
                            "title": "information",
                            "href": "https://toar-data.fz-juelich.de",
                            "hreflang": "en-US"
                        }
                    ],
                    "extents": {
                        "spatial": {
                            "bbox": [
                                -180,
                                -90,
                                180,
                                90
                            ],
                            "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                        }
                    },
                    "providers": [
                        {
                            "type": "connected-systems",
                            "name": "toardb-adapter",
                            "data": "",
                        }
                    ]
                }
        }

    def query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        """
        query the provider

        :returns: dict of formatted systems matching the query parameters
        """

        items, links = self._fetch_all_systems(parameters)

        if len(items) == 0:
            return [], []

        if parameters.format == F_JSON or parameters.format == F_GEOJSON:
            return [self._format_system_json(item) for item in items.values()], links
        else:
            return [self._format_system_sml(item) for item in items.values()], links

    def query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        params = {
            "fields": "id,coordinates",
        }

        if parameters.system is not None:
            # Request SF by system
            params["id"] = parameters.system[0]
        elif parameters.id is not None:
            # Request SF by id
            params["id"] = ",".join(identifier.split("_")[0] for identifier in parameters.id)

        self._parse_paging(parameters, params)

        stations = self.session.get(self.META_URL, params=params).json()

        features = []
        for station in stations:
            features.append({
                "type": "Feature",
                "properties": {
                    "featureType": "http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint",
                    "uid": f"{station['id']}_feature",  # There is no uid so we generate one based on station_id
                    "name": f"SamplingFeature of Station {station['id']}",
                    "sampledFeature@link": {
                        "href": "https://www.opengis.net/def/nil/OGC/0/unknown"
                    }
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        station["coordinates"]["lng"],
                        station["coordinates"]["lat"],
                        station["coordinates"]["alt"],
                    ]
                }
            })

        links_json = []
        if len(stations) == int(parameters.limit):
            # page is fully filled - we assume a nextpage exists
            links_json.append({
                "title": "next",
                "href": f"{self.base_url}/samplingFeatures?"
                        f"limit={parameters.limit}"
                        f"&offset={int(params['offset']) + int(parameters.limit)}"
                        f"&f={parameters.format}",
                "rel": "next"
            })

        return features, links_json

    def query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_datastreams(self, parameters: DatastreamsParams) -> CSAGetResponse:

        params = {}
        if parameters.system is not None:
            # Request by system association
            params["station_id"] = ",".join(parameters.system)
        elif parameters.id is not None:
            # Request SF by id
            params["id"] = ",".join(parameters.id)
        self._parse_paging(parameters, params)

        # TODO: TOAR API only offers exact matching and not intersection of intervals
        # if parameters.phenomenonTimeStart is not None:
        #     params["data_start_date"] = parameters.phenomenonTimeStart
        # if parameters.phenomenonTimeEnd is not None:
        #     params["data_end_date"] = parameters.phenomenonTimeEnd
        # if parameters.resultTimeStart is not None:
        #     params["data_start_date"] = parameters.resultTimeStart
        # if parameters.resultTimeEnd is not None:
        #     params["data_start_date"] = parameters.resultTimeEnd

        timeseries = self.session.get(self.TIMESERIES_URL, params=params).json()

        if parameters.schema:
            series = timeseries[0]
            schema = [{
                "obsFormat": "application/om+json",
                "resultSchema": {
                    "id": str(series["variable"]["id"]),
                    "name": series["variable"]["name"],
                    "type": "Quantity",
                    "label": series["variable"]["displayname"],
                    "definition": series["variable"]["cf_standardname"],
                    "uom": {
                        "code": series["variable"]['units']
                    },
                    "description": f"chemical_formula:{series['variable']['chemical_formula']};"
                                   f"longname:{series['variable']['longname']};"
                },
                "parametersSchema": {
                    "type": "DataRecord",
                    "fields": [
                        {
                            "label": "version",
                            "type": "text",
                            "definition": "",
                            "name": "version"
                        },
                        {
                            "label": "flags",
                            "type": "text",
                            "definition": "",
                            "name": "flags"
                        }]
                }
            }]
            return schema, []

        else:
            datastreams = []
            for series in timeseries:
                datastreams.append({
                    "id": str(series["id"]),
                    "name": series["label"],
                    "system@link": {
                        "href": f"{self.base_url}/systems/{series['station']['id']}?f=smljson"
                    },
                    "samplingFeature@link": {
                        "href": f"{self.base_url}/samplingFeatures/{series['station']['id']}_feature?f=geojson"
                    },
                    "formats": [
                        "application/json"
                    ],
                    "outputName": series['variable']['name'],
                    "phenomenonTime": [
                        series['data_start_date'],
                        series['data_end_date']
                    ],
                    "phenomenonTimeInterval": series['sampling_frequency'],
                    "resultTime": [
                        series['data_start_date'],
                        series['data_end_date']
                    ],
                    "resultTimeInterval": series['sampling_frequency'],
                    "resultType": series['data_origin_type'],
                    "live": False,
                    "links": [
                        {
                            "href": f"{self.TIMESERIES_URL}id/{series['id']}",
                            "hreflang": "en-US",
                            "title": "TOARDB FastAPI REST API",
                            "type": "application/json"
                        }
                    ]
                })

            # check if a nextPage exists and potentially add link
            links_json = []
            if len(timeseries) == int(parameters.limit):
                # page is fully filled - we assume a nextpage exists
                links_json.append({
                    "title": "next",
                    "href": f"{self.base_url}/datastreams?"
                            f"limit={parameters.limit}"
                            f"&offset={int(params['offset']) + int(parameters.limit)}"
                            f"&f={parameters.format}",
                    "rel": "next"
                })

            return datastreams, links_json

    def query_observations(self, parameters: ObservationsParams) -> CSAGetResponse:
        params = {}
        self._parse_paging(parameters, params)

        url = self.DATA_URL
        nexturl = self.base_url
        if parameters.datastream:
            # filter by timeseries
            url += parameters.datastream
            nexturl += parameters.datastream

        response = self.session.get(url, params=params).json()
        result = response if not parameters.datastream else response['data']
        observations = [self._format_observation_om_json(obs) for obs in result]

        links_json = []
        if len(observations) == int(parameters.limit):
            # page is fully filled - we assume a nextpage exists
            links_json.append({
                "title": "next",
                "href": f"{nexturl}/observations?"
                        f"limit={parameters.limit}"
                        f"&offset={int(params['offset']) + int(parameters.limit)}"
                        f"&f={parameters.format}",
                "rel": "next"
            })
        return observations, links_json

    def _format_observation_om_json(self, observation: json) -> Dict:
        return {
            # There is no id so synthesize something
            "id": f"{observation['timeseries_id']}_{observation['datetime']}_{observation['version'].strip()}",
            "datastream@id": observation['timeseries_id'],
            "phenomenonTime": observation['datetime'],
            "resultTime": observation['datetime'],
            "result": observation['value'],
            "parameters": {
                "version": observation['version'],
                "flags": observation['flags']
            }
        }

    def _format_system_json(self, station_meta: json) -> Dict:
        """
        Reformat TOAR-DB v2 Json Structure to SensorML 2.0 JSON
        :param station_meta: json description of station
        :return: system dict
        """

        return {
            "type": "Feature",
            "links": [{
                "href": f"{self.META_URL}id/{station_meta['id']}",
                "hreflang": "en-US",
                "title": "TOARDB FastAPI REST API",
                "type": "application/json"
            }],
            "geometry": {
                "type": "Point",
                "coordinates": [
                    station_meta["coordinates"]["lng"],
                    station_meta["coordinates"]["lat"],
                    station_meta["coordinates"]["alt"],
                ]
            },
            "id": station_meta['id'],
            "properties": {
                "description": "",
                "featureType": "http://www.w3.org/ns/sosa/Platform",
                "name": station_meta["name"],
                "uid": station_meta["id"],
            }
        }

    def _format_system_sml(self, station_meta: json):
        """
        Reformat TOAR-DB v2 Json Structure to SensorML 2.0 JSON
        :param data: raw data
        :return: system dict
        """

        metadata = {
            **{
                "coordinate_validation_status": station_meta["coordinate_validation_status"],
                "country": station_meta["country"],
                "state": station_meta["state"],
                # station_meta["type_of_environment"],
                "type_of_area": station_meta["type_of_area"],
                "timezone": station_meta["timezone"]
            },
            **station_meta["globalmeta"]
        }
        # transform to str
        metadata["station_id"] = str(metadata["station_id"])

        characteristics = []
        for key, val in metadata.items():
            t = "Quantity" if isinstance(val, numbers.Number) else "Text"
            elem = {
                "type": t,
                "label": key,
                "definition": self.OWL_LOOKUP[key],
                "value": val
            }
            if t == "Quantity":
                elem["uom"] = {
                    "code": self.UOM_LOOKUP[key]
                }
            characteristics.append(elem)

        system = {
            "type": "PhysicalSystem",
            "id": str(station_meta["id"]),
            "uniqueId": str(station_meta["id"]),
            "name": station_meta["name"],
            "label": station_meta["name"],
            "definition": "http://www.w3.org/ns/sosa/Platform",
            "identifiers":
                [{
                    "label": f"code_{num}",
                    "value": code
                } for num, code in enumerate(station_meta["codes"])],
            "position": {
                "type": "Point",
                "coordinates": [
                    station_meta["coordinates"]["lng"],
                    station_meta["coordinates"]["lat"],
                    station_meta["coordinates"]["alt"],
                ],
            },
            "characteristics": [
                {
                    "id": "globalmeta",
                    "characteristics": characteristics
                    },
            ],
            "outputs": [
                {
                    "id": str(val["id"]),
                    "name": val["name"],
                    "type": "Quantity",
                    "label": val["displayname"],
                    "definition": val["cf_standardname"],
                    "uom": {
                        "code": val['units']
                    },
                    "description": f"chemical_formula:{val['chemical_formula']};longname:{val['longname']};"
                } for val in station_meta["outputs"]
            ],
            "links": [
                {
                    "href": f"{self.META_URL}id/{station_meta['id']}",
                    "hreflang": "en-US",
                    "title": "TOARDB FastAPI REST API",
                    "type": "application/json"
                }
            ]
        }

        return system

    def _fetch_all_systems(self, parameters: SystemsParams) -> (Dict, List):
        params = {}

        params["id"] = parameters.id

        # Parse Query Parameters
        self._parse_paging(parameters, params)
        self._parse_bbox(parameters, params)

        stations = self.session.get(self.META_URL, params=params).json()

        # check if a nextPage exists and potentially add link
        links_json = []
        if len(stations) == int(parameters.limit):
            # page is fully filled - we assume a nextpage exists
            links_json.append({
                "title": "next",
                "href": f"{self.base_url}/systems?"
                        f"limit={parameters.limit}"
                        f"&offset={int(params['offset']) + int(parameters.limit)}"
                        f"&f={parameters.format}",
                "rel": "next"
            })

        stationmap = {}
        for station in stations:
            station["outputs"] = []
            stationmap[station["id"]] = station

        # fetch individual timeseries
        params = {
            "station_id": ','.join([str(k) for k in stationmap.keys()]),
            "limit": 10000,
        }

        timeseries = self.session.get(self.TIMESERIES_URL, params=params).json()
        for series in timeseries:
            station_id = series["station"]["id"]
            stationmap[station_id]["outputs"].append(series["variable"])

        return stationmap, links_json

    def _parse_paging(self, parameters: CSAParams, parsed: Dict) -> None:
        parsed["limit"] = parameters.limit
        parsed["offset"] = int(parameters.offset)

    def _parse_bbox(self, parameters: SystemsParams, parsed: Dict) -> None:
        if parameters.bbox is not None:
            # TODO: throw error on non-default bbox-crs
            # TODO: throw error on invalid coordinates
            coordinates = parameters.bbox.split(",")
            parsed["bounding_box"] = ",".join(coordinates[:3])

            if len(coordinates) > 4:
                parsed["altitude"] = ",".join(coordinates[3:])
