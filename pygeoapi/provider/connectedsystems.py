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
from datetime import datetime, timezone
from typing import List, Optional, Dict, Type, Tuple


class CSAParams:
    format: str
    id: List[str] = None
    q: Optional[List[str]] = None
    limit: int = 10
    offset: int = 0  # non-standard


class CommonParams(CSAParams):
    datetime: Optional[str] = None
    foi: Optional[List[str]] = None
    observed_property: Optional[List[str]] = None


class SystemsParams(CommonParams):
    bbox: Optional[str] = None
    geom: Optional[str] = None
    parent: Optional[List[str]] = None
    procedure: Optional[List[str]] = None
    controlled_property: Optional[List[str]] = None


class DeploymentsParams(CommonParams):
    bbox: Optional[str] = None
    geom: Optional[str] = None
    system: Optional[List[str]] = None


class ProceduresParams(CommonParams):
    controlled_property: Optional[List[str]] = None


class SamplingFeaturesParams(CommonParams):
    bbox: Optional[str] = None
    geom: Optional[str] = None
    controlled_property: Optional[List[str]] = None
    system: Optional[List[str]] = None


class DatastreamsParams(CommonParams):
    phenomenonTimeStart: Optional[str] = None
    phenomenonTimeEnd: Optional[str] = None
    resultTimeStart: Optional[str] = None
    resultTimeEnd: Optional[str] = None
    system: Optional[List[str]] = None
    schema: Optional[bool] = None


def parse_query_parameters(type, params: Dict):
    def _parse_list(identifier):
        if params.get(identifier) is not None:
            parsed[identifier] = []
            for elem in params.get(identifier).split(","):
                parsed[identifier].append(elem)

    def _parse_time(raw, start, end):
        # TODO: Support 'latest' qualifier
        now = datetime.utcnow().isoformat("T") + "Z"
        if "/" in raw:
            # time interval
            split = raw.split("/")
            startts = split[0]
            endts = split[1]
            if startts == "now":
                parsed[start] = now
            elif startts == "..":
                parsed[start] = None
            else:
                parsed[start] = raw
            if endts == "now":
                parsed[end] = now
            elif endts == "..":
                parsed[end] = None
            else:
                parsed[end] = raw
        else:
            if raw == "now":
                parsed[start] = now
                parsed[end] = now
            else:
                parsed[start] = raw
                parsed[end] = raw

    parsed = {}

    _parse_list("id")
    _parse_list("system")
    _parse_list("parent")
    _parse_list("observed_property")
    _parse_list("procedure")
    _parse_list("controlled_property")
    _parse_list("foi")

    if params.get("format") is not None:
        parsed["format"] = params.get("format")

    if params.get("limit") is not None:
        parsed["limit"] = params.get("limit")

    if params.get("offset") is not None:
        parsed["offset"] = params.get("offset")

    if params.get("bbox") is not None:
        parsed["bbox"] = params.get("bbox")

    if params.get("datetime") is not None:
        parsed["datetime"] = params.get("datetime")

    if params.get("geom") is not None:
        parsed["geom"] = params.get("geom")

    if params.get("phenomenonTime") is not None:
        _parse_time(params.get("phenomenonTime"), "phenomenonTimeStart", "phenomenonTimeEnd")

    if params.get("resultTime") is not None:
        _parse_time(params.get("resultTime"), "resultTimeStart", "resultTimeEnd")

    for name, p in parsed.items():
        if hasattr(type, name):
            setattr(type, name, p)
    return type


CSAResponse = Tuple[List[Dict], List[Dict]]


class ConnectedSystemsBaseProvider:
    """Base provider for Providers implemented Connected Systems API"""

    def __init__(self, provider_def):
        pass

    def get_conformance(self) -> List[str]:
        """Returns the list of conformance classes that are implemented by this provider"""
        return []

    def get_collections(self) -> Dict[str, Dict]:
        """Returns the list of collections that are served by this provider"""
        return {}

    def query_systems(self, parameters: SystemsParams) -> CSAResponse:
        """
        implements queries on systems as specified in openapi-connectedsystems-1

        :returns: dict of formatted systems matching the query parameters
        """

        raise NotImplementedError()

    def query_deployments(self, parameters: DeploymentsParams) -> CSAResponse:
        """
        implements queries on deployments as specified in openapi-connectedsystems-1

        :returns: dict of formatted deployments matching the query parameters
        """

        raise NotImplementedError()

    def query_procedures(self, parameters: ProceduresParams) -> CSAResponse:
        """
        implements queries on procedures as specified in openapi-connectedsystems-1

        :returns: dict of formatted procedures matching the query parameters
        """

        raise NotImplementedError()

    def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAResponse:
        """
        implements queries on samplingFeatures as specified in openapi-connectedsystems-1

        :returns: dict of formatted samplingFeatures matching the query parameters
        """

        raise NotImplementedError()

    def query_properties(self, parameters: CSAParams) -> CSAResponse:
        """
        implements queries on properties as specified in openapi-connectedsystems-1

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def query_properties(self, parameters: CSAParams) -> CSAResponse:
        """
        implements queries on properties as specified in openapi-connectedsystems-1

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def query_datastreams(self, parameters: DatastreamsParams) -> CSAResponse:
        """
        implements queries on properties as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def query_observations(self, parameters: CSAParams) -> CSAResponse:
        """
        implements queries on properties as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def create(self, item):
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        raise NotImplementedError()

    def update(self, identifier, item):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        raise NotImplementedError()

    def delete(self, identifier):
        """
        Deletes an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """

        raise NotImplementedError()
