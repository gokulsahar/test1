from pype.core.registry.sqlite_backend import ComponentRegistry
from pype.components.io.echo import EchoComponent

registry = ComponentRegistry()
component_metadata = EchoComponent.get_metadata()
registry.register_component(component_metadata)