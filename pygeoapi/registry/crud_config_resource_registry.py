import sqlite3
import logging
import json

from pygeoapi.registry.resource_registry import ResourceRegistry
from pygeoapi.util import (filter_dict_by_key_value, get_provider_by_type)
"""
This registry implementation allows creation, update and removal of resources
"""

LOGGER = logging.getLogger(__name__)

class CrudConfigResourceRegistry(ResourceRegistry):

    db_name = 'resources'

    def __init__(self, plugin_def: dict):
        super(CrudConfigResourceRegistry, self).__init__(plugin_def)
        resources = plugin_def['resources']
        
        self.resources_change_listeners = plugin_def['resources_change_listeners']

        try:
            # set up the database as a shared storage across workers
            self.db_connection = sqlite3.connect("/tmp/pygeoapi_resources2.db")
            cur = self.db_connection.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {CrudConfigResourceRegistry.db_name};')
            cur.execute(f'CREATE TABLE IF NOT EXISTS {CrudConfigResourceRegistry.db_name}(name text PRIMARY KEY, configuration text);')
            
            for k, v in resources.items():            
                cur.execute(f'INSERT INTO {CrudConfigResourceRegistry.db_name} VALUES(?, ?)', (k, json.dumps(v)))
            
            LOGGER.info(f"Loaded {len(resources.items())} resources into SQLite")
            cur.close()
            self.db_connection.commit()
            
        except Exception as e:
            LOGGER.info('database locked: ' + str(e))
        
    
    def get_all_resources(self) -> dict:
        cur = self.db_connection.cursor()
        cur.execute(f'SELECT * from {CrudConfigResourceRegistry.db_name};')
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r[0]] = json.loads(r[1])
        cur.close()

        LOGGER.info(f"get all resources: {result}")

        return result
    
    def get_resources_of_type(self, target_type: str) -> dict:
        return filter_dict_by_key_value(self.get_all_resources(), 'type', target_type)
    
    def get_resource_provider_of_type(self, resource_name: str,
                             provider_type: str) -> dict:
        return get_provider_by_type(self.get_all_resources()[resource_name]['providers'],
                                    provider_type)

    def set_resource_config(self, resource_name: str,
                            configuration: dict) -> None:
        cur = self.db_connection.cursor()
        cur.execute(f'INSERT INTO {CrudConfigResourceRegistry.db_name} VALUES(?, ?)', (resource_name, json.dumps(configuration)))
        cur.close()
        self.db_connection.commit()
        self.call_change_listeners(self.get_all_resources())
    
    def delete_resource_config(self, resource_name: str) -> None:
        if resource_name in self.resources:
            del self.resources[resource_name]
        
            self.call_change_listeners(self.resources)
    
    def get_resource_config(self, resource_name: str) -> dict:
        res = self.get_all_resources()
        if resource_name in res:
            return res[resource_name]
        else:
            return None

    def call_change_listeners(self, new_resources):
        for cl in self.resources_change_listeners:
            cl.on_resources_changed(new_resources)