from pype.core.registry.component_registry import ComponentRegistry
from pype.core.registry.joblet_registry import JobletRegistry
from pype.components.io.echo import EchoComponent

registry = ComponentRegistry()
registry1 = JobletRegistry()
component_metadata = EchoComponent.get_metadata()
registry.register_component(component_metadata)