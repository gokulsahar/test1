"""
SubJobTracker for DataPY engine execution coordination.

Tracks component execution state within subjobs and determines when
to fire SUBJOB_OK/SUBJOB_ERR tokens for downstream subjob coordination.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Set, Optional
from enum import Enum


class ComponentState(Enum):
    """Component execution states within a subjob."""
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class SubJobTracker:
    """
    Tracks execution state of all components within a subjob.
    
    Determines when to fire SUBJOB_OK/SUBJOB_ERR tokens based on
    component completion states according to fail_strategy.
    """
    subjob_id: str
    components: Set[str]
    fail_strategy: str = "halt"  # "halt" or "continue"
    logger: Optional[logging.Logger] = None
    
    # Internal state tracking
    component_states: Dict[str, ComponentState] = field(default_factory=dict)
    succeeded_count: int = 0
    failed_count: int = 0
    total_components: int = 0
    
    # Token firing state
    ok_fired: bool = False
    error_fired: bool = False
    
    def __post_init__(self):
        """Initialize component states and counts."""
        # Convert list to set if needed
        if isinstance(self.components, list):
            self.components = set(self.components)
        
        self.total_components = len(self.components)
        
        # Initialize all components as WAITING
        for component in self.components:
            self.component_states[component] = ComponentState.WAITING
        
        if self.logger:
            self.logger.debug(
                "SUBJOB_TRACKER_INITIALIZED",
                extra={
                    "subjob_id": self.subjob_id,
                    "total_components": self.total_components,
                    "components": list(self.components),
                    "fail_strategy": self.fail_strategy
                }
            )
        
    def notify_component_start(self, component: str) -> None:
        """
        Notify that a component has started execution.
        
        Args:
            component: Component name that started
        """
        if component not in self.components:
            if self.logger:
                self.logger.warning(
                    "SUBJOB_TRACKER_UNKNOWN_COMPONENT",
                    extra={
                        "subjob_id": self.subjob_id,
                        "component": component,
                        "action": "start"
                    }
                )
            return
        
        self.component_states[component] = ComponentState.RUNNING
        
        if self.logger:
            self.logger.debug(
                "SUBJOB_COMPONENT_STARTED",
                extra={
                    "subjob_id": self.subjob_id,
                    "component": component,
                    "state": "RUNNING"
                }
            )
    
    def notify_component_success(self, component: str) -> None:
        """
        Notify that a component completed successfully.
        
        Args:
            component: Component name that succeeded
        """
        if component not in self.components:
            if self.logger:
                self.logger.warning(
                    "SUBJOB_TRACKER_UNKNOWN_COMPONENT",
                    extra={
                        "subjob_id": self.subjob_id,
                        "component": component,
                        "action": "success"
                    }
                )
            return
        
        # Only count if not already succeeded
        if self.component_states[component] != ComponentState.SUCCEEDED:
            self.component_states[component] = ComponentState.SUCCEEDED
            self.succeeded_count += 1
            
            if self.logger:
                self.logger.debug(
                    "SUBJOB_COMPONENT_SUCCEEDED",
                    extra={
                        "subjob_id": self.subjob_id,
                        "component": component,
                        "succeeded_count": self.succeeded_count,
                        "total_components": self.total_components
                    }
                )
    
    def notify_component_failure(self, component: str, error_type: str = "unknown") -> None:
        """
        Notify that a component failed.
        
        Args:
            component: Component name that failed
            error_type: Type of error for logging
        """
        if component not in self.components:
            if self.logger:
                self.logger.warning(
                    "SUBJOB_TRACKER_UNKNOWN_COMPONENT",
                    extra={
                        "subjob_id": self.subjob_id,
                        "component": component,
                        "action": "failure"
                    }
                )
            return
        
        # Only count if not already failed
        if self.component_states[component] != ComponentState.FAILED:
            self.component_states[component] = ComponentState.FAILED
            self.failed_count += 1
            
            if self.logger:
                self.logger.info(
                    "SUBJOB_COMPONENT_FAILED",
                    extra={
                        "subjob_id": self.subjob_id,
                        "component": component,
                        "error_type": error_type,
                        "failed_count": self.failed_count,
                        "fail_strategy": self.fail_strategy
                    }
                )
    
    def notify_component_skipped(self, component: str, reason: str = "dependency_failed") -> None:
        """
        Notify that a component was skipped.
        
        Args:
            component: Component name that was skipped
            reason: Reason for skipping
        """
        if component not in self.components:
            return
        
        self.component_states[component] = ComponentState.SKIPPED
        
        if self.logger:
            self.logger.debug(
                "SUBJOB_COMPONENT_SKIPPED",
                extra={
                    "subjob_id": self.subjob_id,
                    "component": component,
                    "reason": reason
                }
            )
    
    def should_fire_ok(self) -> bool:
        """
        Check if SUBJOB_OK token should be fired.
        
        Returns:
            True if all components succeeded and OK not yet fired
        """
        if self.ok_fired or self.failed_count > 0:
            return False
        
        # All components must be in final state (succeeded or skipped)
        final_states = {ComponentState.SUCCEEDED, ComponentState.SKIPPED}
        all_final = all(state in final_states for state in self.component_states.values())
        
        # At least one component must have succeeded
        has_success = self.succeeded_count > 0
        
        return all_final and has_success
    
    def should_fire_error(self) -> bool:
        """
        Check if SUBJOB_ERR token should be fired.
        
        Returns:
            True if subjob should be considered failed and ERR not yet fired
        """
        if self.error_fired:
            return False
        
        if self.fail_strategy == "halt":
            # Fire immediately on first failure
            return self.failed_count > 0
        
        elif self.fail_strategy == "continue":
            # Fire only when all components are done and at least one failed
            final_states = {ComponentState.SUCCEEDED, ComponentState.FAILED, ComponentState.SKIPPED}
            all_final = all(state in final_states for state in self.component_states.values())
            return all_final and self.failed_count > 0
        
        return False
    
    def fire_ok(self) -> bool:
        """
        Fire SUBJOB_OK token if conditions are met.
        
        Returns:
            True if token was fired, False if already fired or conditions not met
        """
        if self.should_fire_ok():
            self.ok_fired = True
            
            if self.logger:
                self.logger.info(
                    "SUBJOB_OK_FIRED",
                    extra={
                        "subjob_id": self.subjob_id,
                        "succeeded_components": self.succeeded_count,
                        "total_components": self.total_components,
                        "token": f"SUBJOB_OK::{self.subjob_id}"
                    }
                )
            return True
        
        return False
    
    def fire_error(self) -> bool:
        """
        Fire SUBJOB_ERR token if conditions are met.
        
        Returns:
            True if token was fired, False if already fired or conditions not met
        """
        if self.should_fire_error():
            self.error_fired = True
            
            if self.logger:
                self.logger.info(
                    "SUBJOB_ERROR_FIRED",
                    extra={
                        "subjob_id": self.subjob_id,
                        "failed_components": self.failed_count,
                        "succeeded_components": self.succeeded_count,
                        "fail_strategy": self.fail_strategy,
                        "token": f"SUBJOB_ERR::{self.subjob_id}"
                    }
                )
            return True
        
        return False
    
    def get_status(self) -> Dict[str, any]:
        """
        Get current subjob status for monitoring.
        
        Returns:
            Status dictionary with counts and state
        """
        waiting_count = sum(1 for state in self.component_states.values() 
                          if state == ComponentState.WAITING)
        running_count = sum(1 for state in self.component_states.values() 
                          if state == ComponentState.RUNNING)
        skipped_count = sum(1 for state in self.component_states.values() 
                          if state == ComponentState.SKIPPED)
        
        return {
            "subjob_id": self.subjob_id,
            "total_components": self.total_components,
            "waiting": waiting_count,
            "running": running_count,
            "succeeded": self.succeeded_count,
            "failed": self.failed_count,
            "skipped": skipped_count,
            "ok_fired": self.ok_fired,
            "error_fired": self.error_fired,
            "fail_strategy": self.fail_strategy,
            "is_complete": waiting_count == 0 and running_count == 0
        }
    
    def is_complete(self) -> bool:
        """
        Check if subjob execution is complete.
        
        Returns:
            True if all components are in final state
        """
        final_states = {ComponentState.SUCCEEDED, ComponentState.FAILED, ComponentState.SKIPPED}
        return all(state in final_states for state in self.component_states.values())
    
    def get_failed_components(self) -> Set[str]:
        """Get set of components that failed."""
        return {comp for comp, state in self.component_states.items() 
                if state == ComponentState.FAILED}
    
    def get_succeeded_components(self) -> Set[str]:
        """Get set of components that succeeded."""
        return {comp for comp, state in self.component_states.items() 
                if state == ComponentState.SUCCEEDED}
        
        
        
    def notify(self, component: str, state: ComponentState) -> None:
        """
        Generic notify method for state changes.
        
        Args:
            component: Component name
            state: New component state
        """
        if state == ComponentState.RUNNING:
            self.notify_component_start(component)
        elif state == ComponentState.SUCCEEDED:
            self.notify_component_success(component)
        elif state == ComponentState.FAILED:
            self.notify_component_failure(component)
        elif state == ComponentState.SKIPPED:
            self.notify_component_skipped(component)