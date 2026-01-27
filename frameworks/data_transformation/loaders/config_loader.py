"""ConfigLoader - Load transformation pipeline configurations from JSON or YAML files."""

import json
from pathlib import Path
from typing import Any, Dict, Union


class ConfigLoader:
    """
    Load transformation pipeline configurations from JSON or YAML files.
    
    Reuses the pattern from the validation framework for consistency.
    """
    
    @staticmethod
    def load(path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a file.
        
        Args:
            path: Path to JSON or YAML configuration file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is not supported
            json.JSONDecodeError: If JSON is invalid
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        suffix = path.suffix.lower()
        content = path.read_text()
        
        if suffix == ".json":
            return json.loads(content)
        elif suffix in (".yaml", ".yml"):
            try:
                import yaml
                return yaml.safe_load(content)
            except ImportError:
                raise ImportError(
                    "PyYAML is required for YAML configuration files. "
                    "Install with: pip install pyyaml"
                )
        else:
            raise ValueError(
                f"Unsupported configuration format: {suffix}. "
                "Use .json, .yaml, or .yml"
            )
    
    @staticmethod
    def load_string(content: str, format: str = "json") -> Dict[str, Any]:
        """
        Load configuration from a string.
        
        Args:
            content: Configuration content as string
            format: Format of the content ("json" or "yaml")
            
        Returns:
            Parsed configuration dictionary
        """
        if format == "json":
            return json.loads(content)
        elif format in ("yaml", "yml"):
            try:
                import yaml
                return yaml.safe_load(content)
            except ImportError:
                raise ImportError(
                    "PyYAML is required for YAML configuration files. "
                    "Install with: pip install pyyaml"
                )
        else:
            raise ValueError(f"Unsupported format: {format}")
