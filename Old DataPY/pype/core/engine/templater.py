"""
Runtime Template Resolution for DataPY Engine.

This module handles context, secret, and global variable templating at execution time, allowing
one job build to be executed with different contexts and secrets.
"""

import re
import json
import logging
from pathlib import Path
from typing import Any, Dict, Set, List
from pype.core.utils.constants import (
    CONTEXT_VAR_PATTERN, 
    SECRET_VAR_PATTERN, 
    GLOBAL_VAR_PATTERN,
    GLOBAL_VAR_DELIMITER,
    DEFAULT_ENCODING
)

logger = logging.getLogger(__name__)


class RuntimeTemplateError(Exception):
    """Runtime template resolution error."""
    pass


class SecretResolutionError(RuntimeTemplateError):
    """Secret resolution error."""
    pass


class ContextResolutionError(RuntimeTemplateError):
    """Context variable resolution error."""
    pass


class MissingGlobalVar(RuntimeTemplateError):
    """Missing global variable error."""
    
    def __init__(self, global_key: str):
        self.global_key = global_key
        super().__init__(f"Missing global variable: {global_key}")


class RuntimeTemplater:
    """
    Handles runtime template resolution for job execution.
    
    Resolves context variables, secrets, and global variables in component configurations
    and job metadata at execution time.
    """
    
    def __init__(self, job_folder: Path, context_name: str = None):
        """
        Initialize runtime templater.
        
        Args:
            job_folder: Path to job folder containing assets/context/
            context_name: Name of context to use (e.g., 'dev', 'prod')
        """
        self.job_folder = Path(job_folder)
        self.context_name = context_name
        self.context_data = {}
        self.resolved_secrets = set()  # Track which secrets were resolved
        self.failed_secrets = set()    # Track which secrets failed
        self.missing_context_vars = set()  # Track missing context variables
        
        # Load context data
        self._load_context()
    
    def _load_context(self):
        """Load context data from context file."""
        context_folder = self.job_folder / "assets" / "context"
        
        if not context_folder.exists():
            raise ContextResolutionError(f"Context folder not found: {context_folder}")
        
        # Determine context filename
        job_name = self.job_folder.name
        
        if self.context_name:
            context_filename = f"{job_name}_{self.context_name}.json"
        else:
            context_filename = f"{job_name}_context.json"  # Default context
        
        context_file = context_folder / context_filename
        
        if not context_file.exists():
            available_contexts = [f.stem for f in context_folder.glob("*.json")]
            raise ContextResolutionError(
                f"Context file not found: {context_file}\n"
                f"Available contexts: {available_contexts}"
            )
        
        try:
            with open(context_file, 'r', encoding=DEFAULT_ENCODING) as f:
                self.context_data = json.load(f)
            
            logger.info(f"Loaded context from: {context_file}")
            logger.info(f"Context variables: {list(self.context_data.keys())}")
            
        except json.JSONDecodeError as e:
            raise ContextResolutionError(f"Invalid JSON in context file {context_file}: {e}")
        except Exception as e:
            raise ContextResolutionError(f"Error loading context file {context_file}: {e}")
    
    def resolve_dag_templates(self, dag_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve templates in DAG data (component configs).
        
        Args:
            dag_data: DAG data with potential templates in node configs
            
        Returns:
            DAG data with resolved templates
        """
        logger.info("Resolving templates in DAG data...")
        
        resolved_dag = self._resolve_recursive(dag_data)
        
        # Log resolution summary
        logger.info(f"Template resolution completed:")
        logger.info(f"  Context variables used: {len(self.context_data)}")
        logger.info(f"  Context variables missing: {len(self.missing_context_vars)}")
        logger.info(f"  Secrets resolved: {len(self.resolved_secrets)}")
        
        if self.failed_secrets:
            logger.warning(f"  Failed secrets: {list(self.failed_secrets)}")
        
        if self.missing_context_vars:
            logger.warning(f"  Missing context variables (set to None): {list(self.missing_context_vars)}")
        
        return resolved_dag
    
    def resolve_execution_metadata_templates(self, exec_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve templates in execution metadata.
        
        Args:
            exec_metadata: Execution metadata with potential templates
            
        Returns:
            Execution metadata with resolved templates
        """
        logger.info("Resolving templates in execution metadata...")
        return self._resolve_recursive(exec_metadata)
    
    def resolve_global_variables(self, config: Dict[str, Any], globals_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve global variable templates in component configuration.
        
        Replaces {{component__variable}} patterns with actual values from GlobalStore.
        
        Args:
            config: Component configuration dictionary
            globals_snapshot: Point-in-time snapshot of GlobalStore
            
        Returns:
            New configuration dictionary with resolved global variables
            
        Raises:
            MissingGlobalVar: If referenced global variable doesn't exist
        """
        def resolve_value(value):
            if isinstance(value, str):
                return self._resolve_global_var_string(value, globals_snapshot)
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            else:
                return value
        
        return resolve_value(config)
    
    def _resolve_global_var_string(self, template: str, globals_snapshot: Dict[str, Any]) -> str:
        """
        Resolve global variable patterns in a template string.
        
        Uses existing GLOBAL_VAR_PATTERN from constants.py for {{component__variable}} format.
        
        Args:
            template: String potentially containing {{component__variable}}
            globals_snapshot: GlobalStore snapshot
            
        Returns:
            String with global variables resolved
            
        Raises:
            MissingGlobalVar: If referenced global variable doesn't exist
        """
        def replace_global_var(match):
            # Extract component and variable from match groups using existing pattern
            component_name = match.group(1)
            variable_name = match.group(2)
            
            # Construct global key: component__variable
            global_key = f"{component_name}{GLOBAL_VAR_DELIMITER}{variable_name}"
            
            if global_key not in globals_snapshot:
                raise MissingGlobalVar(global_key)
            
            value = globals_snapshot[global_key]
            logger.debug(f"Resolved global variable: {global_key} = {value}")
            return str(value)
        
        try:
            # Use existing GLOBAL_VAR_PATTERN from constants.py
            return re.sub(GLOBAL_VAR_PATTERN, replace_global_var, template)
        except re.error as e:
            raise RuntimeTemplateError(f"Invalid global variable template pattern: {e}")
    
    def _resolve_recursive(self, data: Any) -> Any:
        """Recursively resolve templates in nested data structures."""
        if isinstance(data, dict):
            return {key: self._resolve_recursive(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._resolve_recursive(item) for item in data]
        elif isinstance(data, str):
            return self._resolve_template_string(data)
        else:
            return data
    
    def _resolve_template_string(self, template: str) -> str:
        """Resolve context and secret variables in a template string."""
        if not isinstance(template, str):
            return str(template)
        
        resolved = template
        
        # Resolve context variables: {{context.variable}}
        resolved = self._resolve_context_variables(resolved)
        
        # Resolve secret variables: {{secret.path.variable}}
        resolved = self._resolve_secret_variables(resolved)
        
        # NOTE: Global variables are NOT resolved here
        # They are resolved separately in resolve_global_variables()
        
        return resolved
    
    def _resolve_context_variables(self, template: str) -> str:
        """Resolve context variables in template string with warning for missing variables."""
        def replace_context_var(match) -> str:
            var_name = match.group(1)
            if var_name not in self.context_data:
                # Log warning and track missing variable
                self.missing_context_vars.add(var_name)
                logger.warning(
                    f"Context variable '{var_name}' not found in context. "
                    f"Setting to None. Available variables: {list(self.context_data.keys())}"
                )
                return "None"  # Replace with string "None"
            
            value = self.context_data[var_name]
            logger.debug(f"Resolved context variable: {var_name} = {value}")
            return str(value)
        
        try:
            return re.sub(CONTEXT_VAR_PATTERN, replace_context_var, template)
        except re.error as e:
            raise RuntimeTemplateError(f"Invalid context template pattern: {e}")
    
    def _resolve_secret_variables(self, template: str) -> str:
        """Resolve secret variables in template string."""
        def replace_secret_var(match) -> str:
            secret_path = match.group(1)
            
            try:
                # TODO: Implement HashiCorp Vault integration
                resolved_value = self._resolve_secret_from_vault(secret_path)
                self.resolved_secrets.add(secret_path)
                
                # DO NOT log secret values - only status
                logger.info(f"Resolved secret: {secret_path} [REDACTED]")
                return resolved_value
                
            except Exception as e:
                self.failed_secrets.add(secret_path)
                logger.error(f"Failed to resolve secret: {secret_path} - {e}")
                raise SecretResolutionError(f"Secret resolution failed for '{secret_path}': {e}")
        
        try:
            return re.sub(SECRET_VAR_PATTERN, replace_secret_var, template)
        except re.error as e:
            raise RuntimeTemplateError(f"Invalid secret template pattern: {e}")
    
    def _resolve_secret_from_vault(self, secret_path: str) -> str:
        """
        Resolve secret from HashiCorp Vault.
        
        TODO: Implement HashiCorp Vault integration.
        
        Args:
            secret_path: Vault path to secret (e.g., "database.prod.password")
            
        Returns:
            Secret value from Vault
            
        Raises:
            SecretResolutionError: If secret cannot be resolved
        """
        # PLACEHOLDER: For now, return placeholder value
        # This will be replaced with actual Vault integration
        
        logger.warning(f"PLACEHOLDER: Secret resolution not yet implemented for: {secret_path}")
        return f"VAULT_SECRET_{secret_path.upper().replace('.', '_')}"
    
    def get_resolution_summary(self) -> Dict[str, Any]:
        """
        Get summary of template resolution for logging/monitoring.
        
        Returns:
            Summary with context info and secret resolution status
        """
        return {
            "context_file": f"{self.job_folder.name}_{self.context_name or 'context'}.json",
            "context_variables": list(self.context_data.keys()),
            "context_values": self.context_data,  # Safe to log context values
            "missing_context_vars": list(self.missing_context_vars),
            "secrets_resolved": list(self.resolved_secrets),
            "secrets_failed": list(self.failed_secrets),
            "total_secrets": len(self.resolved_secrets) + len(self.failed_secrets)
        }


def create_runtime_templater(job_folder: Path, context_name: str = None) -> RuntimeTemplater:
    """
    Factory function to create runtime templater.
    
    Args:
        job_folder: Path to job folder
        context_name: Optional context name (e.g., 'dev', 'prod')
        
    Returns:
        Configured RuntimeTemplater instance
    """
    return RuntimeTemplater(job_folder, context_name)


def resolve_job_templates(job_folder: Path, context_name: str = None) -> Dict[str, Any]:
    """
    Convenience function to resolve all templates in a job folder.
    
    Args:
        job_folder: Path to job folder
        context_name: Optional context name
        
    Returns:
        Dictionary with resolved DAG and execution metadata
    """
    templater = create_runtime_templater(job_folder, context_name)
    
    job_name = job_folder.name
    
    # Load and resolve DAG data
    dag_file = job_folder / f"{job_name}_dag.json"
    with open(dag_file, 'r', encoding=DEFAULT_ENCODING) as f:
        dag_data = json.load(f)
    resolved_dag = templater.resolve_dag_templates(dag_data)
    
    # Load and resolve execution metadata
    exec_file = job_folder / f"{job_name}_execution_metadata.json"
    with open(exec_file, 'r', encoding=DEFAULT_ENCODING) as f:
        exec_metadata = json.load(f)
    resolved_exec_metadata = templater.resolve_execution_metadata_templates(exec_metadata)
    
    return {
        "dag": resolved_dag,
        "execution_metadata": resolved_exec_metadata,
        "resolution_summary": templater.get_resolution_summary()
    }