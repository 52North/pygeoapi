from pygeoapi.registry.resource_registry import ResourcesChangeListener

class OpenApiChangeListener(ResourcesChangeListener):

    def __init__(self):
        pass

    def on_resources_changed(self, new_resources: dict):
        from pygeoapi.openapi import update_openapi_document
        update_openapi_document(new_resources)