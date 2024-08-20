from pygeoapi.registry.resource_registry import ResourceRegistry
from pygeoapi.util import (filter_dict_by_key_value, get_provider_by_type)
"""
This registry implementation is a functional replacement of a config based
pygeoapi instance. Resources cannot be changed and are loaded at service start.
"""
class ConfigResourceRegistry(ResourceRegistry):

    def __init__(self, plugin_def: dict):
        super(ConfigResourceRegistry, self).__init__(plugin_def)
        self.resources = plugin_def['resources']
    
    def get_all_resources(self) -> dict:
        return self.resources
    
    def get_resources_of_type(self, target_type: str) -> dict:
        return filter_dict_by_key_value(self.resources, 'type', target_type)
    
    def get_resource_provider_of_type(self, resource_name: str,
                             provider_type: str) -> dict:
        return get_provider_by_type(self.resources[resource_name]['providers'],
                                    provider_type)

    def set_resource_config(self, resource_name: str,
                            configuration: dict) -> None:
        pass
    
    def delete_resource_config(self, resource_name: str) -> None:
        pass
    
    def get_resource_config(self, resource_name: str) -> dict:
        if resource_name in self.resources:
            return self.resources[resource_name]
        else:
            return None

    def call_change_listeners(self, new_resources):
        pass