import logging
import math
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'distance',
    'title': {
        'en': 'distance',
        'fr': 'distance'
    },
    'description': {
        'en': 'Test process that calculates the distance between two points',
        'fr': 'Test process that calculates the distance between two points',
    },
    'keywords': ['distance', 'test'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'p1': {
            'title': 'Point 1',
            'description': 'First point',
            'schema': {
                'type': 'array'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['point']
        },
        'p2': {
            'title': 'Point 2',
            'description': 'Second point',
            'schema': {
                'type': 'array'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['point']
        }
    },
    'outputs': {
        'distance': {
            'title': 'Distance between the point',
            'description': 'Distance between the two points',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'name': 'World',
            'message': 'An optional message.',
        }
    }
}


class DistanceProcessor(BaseProcessor):
    """Distance Processor example"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.distance.DistanceProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'

        p1 = data.get('p1', None)
        p2 = data.get('p2', None)

        if p1 is None or p2 is None:
            raise ProcessorExecuteError('Cannot process without both points')

        distance = math.sqrt((p2[0]-p1[0])**2+(p2[1]-p1[1])**2)

        outputs = {
            'id': 'distance',
            'value': distance
        }

        return mimetype, outputs

    def __repr__(self):
        return '<DistanceProcessor> {}'.format(self.name)