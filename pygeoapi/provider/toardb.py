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
import requests
from typing import List, Dict, Optional

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

    META_URL = BASEURL + "stationmeta/"

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.toardb.ToarDBProvider
        """

        super().__init__(provider_def)
        self.base_url = provider_def["base_url"]

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

    def query_systems(self, parameters: SystemsParams) -> CSAResponse:
        """
        query the provider

        :returns: dict of formatted systems matching the query parameters
        """

        items, links = self._fetch_all_systems(parameters)

        if len(items) == 0:
            return [], []

        if parameters.format == F_JSON or parameters.format == F_GEOJSON:
            return [self._format_system_json(item) for item in items], links
        else:
            return [self._format_system_sml(item) for item in items], links

    def query_deployments(self, parameters: DeploymentsParams) -> CSAResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_procedures(self, parameters: ProceduresParams) -> CSAResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

    def query_properties(self, parameters: CSAParams) -> CSAResponse:
        if parameters.id is not None:
            raise ProviderItemNotFoundError()
        else:
            return [], []

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
                "rel": "alternate",
                "type": "application/json"
            }],
            "geometry": {
                "type": "Point",
                "coordinates": [
                    station_meta["coordinates"]["lat"],
                    station_meta["coordinates"]["lng"],
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

        system = {
            "type": "PhysicalSystem",
            "id": station_meta["id"],
            "uniqueId": station_meta["id"],
            "name": station_meta["name"],
            "definition": "http://www.w3.org/ns/sosa/Platform",
            "identifiers":
                [{
                    "label": f"code_{num}",
                    "value": code
                } for num, code in enumerate(station_meta["codes"])],
            "position": {
                "type": "Point",
                "coordinates": [
                    station_meta["coordinates"]["lat"],
                    station_meta["coordinates"]["lng"],
                    station_meta["coordinates"]["alt"],
                ],
            },
            "characteristics": [
                {
                    "type": "Quantity" if isinstance(val, numbers.Number) else "Text",
                    "label": key,
                    "definition": self.OWL_LOOKUP[key],
                    "value": val
                } for key, val in metadata.items()
            ]
        }

        return system

    def _fetch_all_systems(self, parameters: SystemsParams) -> (List, List):
        # TODO: add paging
        params = {}

        params["id"] = parameters.id
        # Parse Query Parameters
        self._parse_paging(parameters, params)
        self._parse_bbox(parameters, params)

        response = requests.get(self.META_URL, params=params).json()

        # check if a nextPage exists and potentially add link
        links_json = []
        if len(response) == int(parameters.limit):
            # page is fully filled - we assume a nextpage exists
            links_json.append({
                "title": "next",
                "href": f"{self.base_url}/systems?"
                        f"limit={parameters.limit}"
                        f"&offset={params['offset'] + parameters.limit}"
                        f"&f={parameters.format}",
                "rel": "next"
            })

        return response, links_json

    def _parse_paging(self, parameters: SystemsParams, parsed: Dict) -> None:
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
