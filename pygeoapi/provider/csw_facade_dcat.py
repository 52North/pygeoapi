# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2023 Tom Kralidis
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

import logging
from urllib.parse import urlencode

from owslib import fes
from owslib.csw import CatalogueServiceWeb
from owslib.ows import ExceptionReport
from pygeoapi.provider.csw_facade import CSWFacadeProvider

from pygeoapi.provider.base import (ProviderConnectionError,
                                    ProviderInvalidQueryError,
                                    ProviderItemNotFoundError,
                                    ProviderQueryError)
from pygeoapi.util import bbox2geojsongeometry, crs_transform, get_typed_value

LOGGER = logging.getLogger(__name__)


class CSWFacadeDCATProvider(CSWFacadeProvider):
    """CSW Facade provider"""

    def _get_csw(self) -> CatalogueServiceWeb:
        """
        Helper function to lazy load a CSW

        returns: `owslib.csw.CatalogueServiceWeb`
        """

        try:
            result = CatalogueServiceWeb(self.data, skip_caps=True)
            return result

        except Exception as err:
            err = f'CSW connection error: {err}'
            raise ProviderConnectionError(err)
    def _owslibrecord2record(self, record):
        LOGGER.debug(f'Transforming {record.identifier}')
        time = None
        # conform to https://ogcincubator.github.io/bblocks-ogcapi-records/build/annotated/api/records/v1/schemas/time/schema.yaml
        if record.date:
            if ":" in record.date:
                time = {
                    "timestamp": record.date if record.date[-1] == "Z" else record.date + "Z"
                }
            else:
                time = {
                    "date": record.date
                }

        feature = {
            'id': record.identifier,
            'type': 'Feature',
            'geometry': None,
            'time': time,
            'properties': {},
            'links': [
                self._gen_getrecordbyid_link(record.identifier)
            ]
        }

        LOGGER.debug('Processing record mappings to properties')
        for key, value in self.record_mappings.items():
            prop_value = getattr(record, value[1])
            if prop_value not in [None, [], '']:
                if key == "language":
                    feature['properties'][key] = {"code": prop_value}
                elif key == "updated":
                    feature['properties'][key] = self._format_updated(prop_value)
                else:
                    feature['properties'][key] = prop_value

        if record.bbox is not None:
            LOGGER.debug('Adding bbox')
            bbox = [
                get_typed_value(record.bbox.minx),
                get_typed_value(record.bbox.miny),
                get_typed_value(record.bbox.maxx),
                get_typed_value(record.bbox.maxy)
            ]
            feature['geometry'] = bbox2geojsongeometry(bbox)

        if record.references:
            LOGGER.debug('Adding references as links')
            for link in record.references:
                if link['url']:
                    feature['links'].append({
                        'rel': 'alternate',
                        'title': link['scheme'] if link['scheme'] else "unknown",
                        'href': link['url']
                    })
        if record.uris:
            LOGGER.debug('Adding URIs as links')
            for link in record.uris:
                if link['url']:
                    feature['links'].append({
                        'rel': 'alternate',
                        'title': link['name'] if link['name'] else "unknown",
                        'href': link['url']
                    })

        if record.rights:
            right = ""
            for link in record.rights:
                if link:
                    right += link
            feature["properties"]["rights"] = right

        return feature

    def _format_updated(self, value: str):
        # conform to json-schema type: `date-time`
        if ":" not in value:
            return value + "T00:00:00Z"
        else:
            return value

    def __repr__(self):
        return f'<CSWFacadeDCATProvider> {self.data}'
