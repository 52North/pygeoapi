import logging
import os

import tempfile
import xarray as xr
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from crop_mapping_tool.landsuitmodel import LandSuitMod
from crop_mapping_tool.plant import Plant

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'crop_model',
    'title': {
        'en': 'crop model for OGC DP23'
    },
    'description': {
        'en': 'uses latest 32days forecast from GEPS-model and a crop to predict its growing condition considering temperature and precipitation'
        
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['OGC', 'drought', 'GREPS', 'crop'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'bbox': {
            'title': 'bbox',
            'description': 'max_lat,min_lat,max_lon,min_lon |in WGS84',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['bbox', 'lat,lon']
        },
        'point':{
          'title': 'pint',
            'description': 'lat,lon |in WGS84',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['point', 'lat,lon']
        },
        'crop':{
          'title': 'crop',
            'description': 'crop name',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['crop', 'plant name']
        },
        'format':{
          'title': 'format',
            'description': '''return type one of: 'nc' 'geojson' defaults to 'geojson' ''',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['format', 'ntcdf', 'geojson']
        }
    },
    'outputs': {
        'echo': {
            'title': 'model output',
            'description': 'model output',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/x-netcdf' # TODO hier was ändern? 
            }
        }
    },
    'example': {
        'inputs': {
            'bbox': '34.88,-26.37,72.86,31.99',
            'point': '52.92,7.23',
            'crop': 'wheat',
            'format': 'nc'
        }
    }
}

class CropModelProcessor(BaseProcessor):
    """Crop Model"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.crop_model.CropModelProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/netcdf'
        
        bbox=data.get('bbox')
        point=data.get('point')
        crop=data.get('crop')
        format=data.get('format')

        if bbox is not None:
          coords = [float(i) for i in bbox.split(',')]
          lat_max=coords[0]
          lat_min=coords[1]
          lon_max=coords[2]
          lon_min=coords[3]

        if point is not None:
          points = [float(i) for i in point.split(',')]
          lat = points[0]
          lon = points[1]

        # read environmental data and plant requirements
        full_data = xr.open_dataset('/pygeoapi/data/full_data20230907.nc')
        plant = Plant.from_file(crop, '/pygeoapi/data/plant_json')


        # run crop model
        landsuitmodel = LandSuitMod()
        landsuitmodel.vars_available = {'temperature': 't2m', 'precipitation': 'pwat'}
        landsuitmodel.lat_lng['lat'] = 'latitude'
        landsuitmodel.lat_lng['lng'] = 'longitude'
        landsuitmodel.read_data(full_data)
        vars_requested = ['temperature']
        landsuitmodel.process(plant, vars_requested)

        # slice the data
        if bbox and point:
            raise ValueError('Only one of the arguments "bbox" and "point" should be set.')
        if bbox:
            landsuitmodel.crop_data(bbox)
        if point:
            bbox_str = landsuitmodel.get_bbox_from_point(point)
            landsuitmodel.crop_data(bbox_str)

        # get return type
        ds_return = None
        if format=='nc':
            ds_return = landsuitmodel.return_netCDF()
            with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in native NetCDF format')
                fp.write(ds_return.to_netcdf())
                fp.seek(0)
                ds_return = fp.read()
                return mimetype, ds_return


        if format=='geojson':
            if bbox:
                ds_return = landsuitmodel.return_geojson('/pygeoapi/temp/modelresult.tiff')
            if point:
                ds_return = landsuitmodel.return_geojson('','',False)

            with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in GeoJSON format')
                fp.write(bytes(ds_return, 'UTF-8'))
                fp.seek(0)
                ds_return = fp.read()
                return mimetype, ds_return



    def __repr__(self):
        return f'<CropModelProcessor> {self.name}'
