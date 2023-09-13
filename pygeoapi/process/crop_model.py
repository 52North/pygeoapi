import logging

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
# import xarray as xr
# import tempfile
# import landsuitmodel

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

        if bbox != NULL:
          coords = [float(i) for i in bbox.split(',')]
          lat_max=coords[0]
          lat_min=coords[1]
          lon_max=coords[2]
          lon_min=coords[3]

        if point != NULL:
          points = [float(i) for i in point.split(',')]
          lat = points[0]
          lon = points[1]

        


        # PSEUDO CODE
        # data = xr.read(LATEST_DATA)
        # plant = wenn plant.json da ist laden ansonsten abbrechen
        # landsuitmodel(data, plant)
        # wenn bbox && point
        #   "nur eines erlaubt"  --> abbrechen
        # wenn bbox 
        #   croppen
        # wenn point
        #   croppen


        # wenn format=nc
        #   return
        # sonst
        #   ebenen des xr zusammenfügen zu einer
        #   --> gdal_contour (oder entsprechendes paket)
        #   return

        
        

       
        
        cropped = data.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min, lon_max))


        print(cropped)
        with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in native NetCDF format')
                fp.write(cropped.to_netcdf())
                fp.seek(0)
                nc=fp.read()
                

                return mimetype, nc

    def __repr__(self):
        return f'<CropModelProcessor> {self.name}'