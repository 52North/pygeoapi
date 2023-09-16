from .resource_registry import ResourceRegistry

class ConfigResourceRegistry(ResourceRegistry):

    def __init__(self, plugin_def: dict):
        self.resources = plugin_def['resources']
    
    def get_all_resources(self) -> dict:
        return self.resources

    def set_resource_config(self, collection_name: str, configuration: dict) -> None:
        pass
    
    def delete_resource_config(self, collection_name: str) -> None:
        pass
    
    def get_resource_config(self, collection_name: str) -> dict:
        pass