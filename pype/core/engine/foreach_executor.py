"""
forEach executor for DataPY execution engine.
"""

import logging
from typing import Dict, Any, Union, List, Optional

import pandas as pd
import dask.dataframe as dd

from pype.core.engine.global_store import GlobalStore, BufferedStore
from pype.core.engine.subjob_tracker import SubJobTracker, ComponentState
from pype.core.engine.component_invoker import ComponentInvoker, ExecutionError
from pype.core.engine.memory_manager import MemoryManager


class ForEachExecutor:
    def __init__(
        self,
        component_invoker: ComponentInvoker,
        memory_manager: MemoryManager,
        execution_metadata: Dict[str, Any],
        logger: logging.Logger
    ):
        self.component_invoker = component_invoker
        self.memory_manager = memory_manager
        self.logger = logger
        self.max_iterations = 10000
        self.component_dependencies = execution_metadata['component_dependencies']
        self.port_mapping = execution_metadata['port_mapping']

    async def execute_forEach_subjob(
        self,
        subjob_id: str,
        component_execution_order: List[str],
        context: Dict[str, Any],
        subjob_tracker: SubJobTracker,
        fired_tokens: set,
        subjob_metadata: Dict[str, Any],
        global_store: GlobalStore,
        upstream_outputs: Dict[str, Dict[str, Any]]
    ) -> None:
        iterator_components = subjob_metadata.get('iterator_components', {})
        root_forEach = self._find_root_forEach_component(iterator_components)

        if not root_forEach:
            self.logger.error(
                "NO_ROOT_FOREACH",
                extra={"subjob_id": subjob_id, "iterator_components": list(iterator_components.keys())}
            )
            return

        await self._execute_forEach_iteration(
            root_forEach,
            iterator_components,
            component_execution_order,
            context,
            subjob_tracker,
            fired_tokens,
            global_store,
            upstream_outputs
        )

    async def _execute_forEach_iteration(
        self,
        forEach_component: str,
        iterator_metadata: Dict[str, Any],
        component_execution_order: List[str],
        context: Dict[str, Any],
        subjob_tracker: SubJobTracker,
        fired_tokens: set,
        global_store: GlobalStore,
        upstream_outputs: Dict[str, Dict[str, Any]]
    ) -> None:
        iteration_count = 0
        forEach_metadata = iterator_metadata[forEach_component]
        iteration_scope = forEach_metadata.get('iteration_scope', [])

        buffered_store = BufferedStore(global_store, self.logger, forEach_component)

        while iteration_count < self.max_iterations:
            buffered_store.start_iteration({"iteration": iteration_count})
            iteration_outputs = self._create_iteration_state(upstream_outputs, iteration_scope)
            iteration_edge_remaining = self.memory_manager.init_edge_reference_counts(
                component_execution_order
            )

            try:
                subjob_tracker.notify(forEach_component, ComponentState.RUNNING)
                inputs = self._prepare_component_inputs(forEach_component, iteration_outputs)
                forEach_result = await self.component_invoker.execute_component(
                    forEach_component, context, inputs, buffered_store
                )

                if not forEach_result.success:
                    subjob_tracker.notify(forEach_component, ComponentState.FAILED)
                    break

                if (
                    not forEach_result.outputs
                    or forEach_result.outputs.get("item") is None
                ):
                    subjob_tracker.notify(forEach_component, ComponentState.SUCCEEDED)
                    break

                iteration_outputs[forEach_component] = forEach_result.outputs
                self.memory_manager.update_edge_reference_counts(
                    forEach_component, iteration_edge_remaining, iteration_outputs
                )
                subjob_tracker.notify(forEach_component, ComponentState.SUCCEEDED)

                for component_name in iteration_scope:
                    if component_name == forEach_component:
                        continue

                    if self._can_component_execute(
                        component_name, iteration_outputs, fired_tokens
                    ):
                        subjob_tracker.notify(component_name, ComponentState.RUNNING)
                        inputs = self._prepare_component_inputs(component_name, iteration_outputs)
                        result = await self.component_invoker.execute_component(
                            component_name, context, inputs, buffered_store
                        )
                        if result.success:
                            iteration_outputs[component_name] = result.outputs
                            self.memory_manager.update_edge_reference_counts(
                                component_name, iteration_edge_remaining, iteration_outputs
                            )
                            subjob_tracker.notify(component_name, ComponentState.SUCCEEDED)
                        else:
                            subjob_tracker.notify(component_name, ComponentState.FAILED)

                # merge iteration outputs back
                upstream_outputs.update(iteration_outputs)

                nested_iterators = forEach_metadata.get('nested_iterators', [])
                for nested_forEach in nested_iterators:
                    if nested_forEach in iterator_metadata:
                        await self._execute_forEach_iteration(
                            nested_forEach,
                            iterator_metadata,
                            component_execution_order,
                            context,
                            subjob_tracker,
                            fired_tokens,
                            global_store,
                            upstream_outputs
                        )

                buffered_store.end_iteration()
                iteration_count += 1
                self.memory_manager.maybe_collect_garbage("forEach_iteration")

            except ExecutionError as e:
                subjob_tracker.notify(forEach_component, ComponentState.FAILED)
                break

        flush_results = buffered_store.flush()
        # flush into global store already done, upstream_outputs holds iteration_outputs
        self.logger.info(
            "FOREACH_BUFFERED_STORE_FLUSHED",
            extra={"forEach_component": forEach_component, "variables_flushed": len(flush_results)}
        )

    def _create_iteration_state(
        self,
        upstream_outputs: Dict[str, Dict[str, Any]],
        iteration_scope: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        iteration_outputs = {k: v for k, v in upstream_outputs.items()}
        for comp in iteration_scope:
            if comp not in iteration_outputs:
                iteration_outputs[comp] = {}
        return iteration_outputs

    def _find_root_forEach_component(
        self,
        iterator_components: Dict[str, Any]
    ) -> Optional[str]:
        for forEach_comp, metadata in iterator_components.items():
            if metadata.get('iterator_depth', 0) == 0:
                return forEach_comp
        return None

    def _can_component_execute(
        self,
        component_name: str,
        component_outputs: Dict[str, Dict[str, Any]],
        fired_tokens: set
    ) -> bool:
        deps = self.component_dependencies[component_name]
        data_ready = all(dep in component_outputs for dep in deps['data'])
        control_ready = all(f"{dep}::ok" in fired_tokens for dep in deps['control'])
        port_info = self.port_mapping[component_name]
        data_available = all(
            source in component_outputs and any(
                port_name in component_outputs[source] for port_name, _ in port_info['inputs']
            )
            for _, source in port_info['inputs']
        )
        return data_ready and control_ready and data_available

    def _prepare_component_inputs(
        self,
        component_name: str,
        component_outputs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Union[pd.DataFrame, dd.DataFrame, List[Union[pd.DataFrame, dd.DataFrame]]]]:
        inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame, List[Union[pd.DataFrame, dd.DataFrame]]]] = {}
        port_info = self.port_mapping[component_name]

        for port_name, source in port_info['inputs']:
            if source in component_outputs:
                for output_port, data in component_outputs[source].items():
                    if output_port == port_name or output_port == 'main':
                        if port_name in inputs:
                            if not isinstance(inputs[port_name], list):
                                inputs[port_name] = [inputs[port_name]]
                            inputs[port_name].append(data)
                        else:
                            inputs[port_name] = data
                        break

        return inputs  # type: ignore
