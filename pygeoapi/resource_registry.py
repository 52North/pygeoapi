class ResourceRegistry:

    def __init__(self, initial_resources: dict):
        pass
    
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