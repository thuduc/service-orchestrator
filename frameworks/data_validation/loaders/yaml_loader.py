"""YAML configuration loader."""

from pathlib import Path
from typing import Any, Dict, Union

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class YamlLoader:
    """
    Loads configuration from YAML files.
    
    Supports loading both pipeline configurations and custom check definitions
    from YAML format files.
    
    Note: Requires PyYAML package to be installed. If not available,
    attempting to use this loader will raise ImportError.
    """

    @staticmethod
    def _check_yaml_available() -> None:
        """Check if PyYAML is available."""
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required for YAML configuration loading. "
                "Install it with: pip install pyyaml"
            )

    @staticmethod
    def load(path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.
        
        Args:
            path: Path to the YAML configuration file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ImportError: If PyYAML is not installed
            yaml.YAMLError: If the file contains invalid YAML
        """
        YamlLoader._check_yaml_available()
        
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @staticmethod
    def load_string(content: str) -> Dict[str, Any]:
        """
        Load configuration from a YAML string.
        
        Args:
            content: YAML string to parse
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            ImportError: If PyYAML is not installed
            yaml.YAMLError: If the string contains invalid YAML
        """
        YamlLoader._check_yaml_available()
        return yaml.safe_load(content)

    @staticmethod
    def save(
        config: Dict[str, Any], 
        path: Union[str, Path], 
        default_flow_style: bool = False
    ) -> None:
        """
        Save configuration to a YAML file.
        
        Args:
            config: Configuration dictionary to save
            path: Path to save the configuration to
            default_flow_style: Use flow style for nested structures (default: False)
            
        Raises:
            ImportError: If PyYAML is not installed
        """
        YamlLoader._check_yaml_available()
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=default_flow_style)
