class ResourceRegistry:

    def __init__(self, plugin_def: dict):
        self.resources_change_listeners = plugin_def['resources_change_listeners']
    
    def get_all_resources(self) -> dict:
        pass
    
    def get_resources_of_type(self, target_type: str) -> dict:
        pass

    def get_resource_provider_of_type(self, resource_name: str,
                             provider_type: str) -> dict:
        pass

    def set_resource_config(self, resource_name: str,
                            configuration: dict) -> None:
        pass
    
    def delete_resource_config(self, resource_name: str) -> None:
        pass
    
    def get_resource_config(self, resource_name: str) -> dict:
        pass
    
    def call_change_listeners(self, new_resources):
        for cl in self.resources_change_listeners:
            cl.on_resources_changed(new_resources)

        
class ResourcesChangeListener:
    
    def __init__(self):
        pass
    
    def on_resources_changed(self, new_resources: dict):
        pass