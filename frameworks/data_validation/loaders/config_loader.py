"""Unified configuration loader for the Data Validation Framework."""

from pathlib import Path
from typing import Any, Dict, Optional, Union

from frameworks.data_validation.loaders.json_loader import JsonLoader
from frameworks.data_validation.loaders.yaml_loader import YamlLoader


class ConfigLoader:
    """
    Unified configuration loader that automatically detects file format.
    
    Supports both JSON and YAML configuration files, selecting the
    appropriate loader based on file extension.
    """

    # Supported file extensions mapped to loaders
    _EXTENSION_MAP = {
        ".json": JsonLoader,
        ".yaml": YamlLoader,
        ".yml": YamlLoader,
    }

    @classmethod
    def load(cls, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a file, auto-detecting format.
        
        Args:
            path: Path to the configuration file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file extension is not supported
        """
        path = Path(path)
        extension = path.suffix.lower()
        
        if extension not in cls._EXTENSION_MAP:
            supported = ", ".join(cls._EXTENSION_MAP.keys())
            raise ValueError(
                f"Unsupported configuration file format: {extension}. "
                f"Supported formats: {supported}"
            )
        
        loader_class = cls._EXTENSION_MAP[extension]
        return loader_class.load(path)

    @classmethod
    def load_pipelines(cls, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load pipeline configurations from a file.
        
        Expects a file with a 'validation_pipelines' key containing
        pipeline definitions.
        
        Args:
            path: Path to the pipelines configuration file
            
        Returns:
            Dictionary of pipeline configurations keyed by pipeline ID
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the configuration doesn't contain 'validation_pipelines'
        """
        config = cls.load(path)
        
        if "validation_pipelines" not in config:
            raise ValueError(
                f"Configuration file {path} must contain a 'validation_pipelines' key"
            )
        
        return config["validation_pipelines"]

    @classmethod
    def load_checks(cls, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load custom check configurations from a file.
        
        Expects a file with a 'custom_checks' key containing
        check definitions.
        
        Args:
            path: Path to the checks configuration file
            
        Returns:
            Dictionary of check configurations keyed by check ID
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the configuration doesn't contain 'custom_checks'
        """
        config = cls.load(path)
        
        if "custom_checks" not in config:
            raise ValueError(
                f"Configuration file {path} must contain a 'custom_checks' key"
            )
        
        return config["custom_checks"]

    @classmethod
    def merge_configs(
        cls,
        *configs: Dict[str, Any],
        deep_merge: bool = True
    ) -> Dict[str, Any]:
        """
        Merge multiple configuration dictionaries.
        
        Later configs override earlier ones for conflicting keys.
        
        Args:
            *configs: Configuration dictionaries to merge
            deep_merge: If True, recursively merge nested dictionaries
            
        Returns:
            Merged configuration dictionary
        """
        result: Dict[str, Any] = {}
        
        for config in configs:
            if deep_merge:
                result = cls._deep_merge(result, config)
            else:
                result.update(config)
        
        return result

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Dictionary with values to override
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = ConfigLoader._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result

    @classmethod
    def save(
        cls,
        config: Dict[str, Any],
        path: Union[str, Path],
        format: Optional[str] = None
    ) -> None:
        """
        Save configuration to a file.
        
        Args:
            config: Configuration dictionary to save
            path: Path to save the configuration to
            format: Force output format ('json' or 'yaml'). If None, auto-detect from extension.
            
        Raises:
            ValueError: If the format is not supported
        """
        path = Path(path)
        
        if format:
            format = format.lower()
            if format == "json":
                JsonLoader.save(config, path)
            elif format in ("yaml", "yml"):
                YamlLoader.save(config, path)
            else:
                raise ValueError(f"Unsupported format: {format}")
        else:
            extension = path.suffix.lower()
            if extension not in cls._EXTENSION_MAP:
                supported = ", ".join(cls._EXTENSION_MAP.keys())
                raise ValueError(
                    f"Unsupported configuration file format: {extension}. "
                    f"Supported formats: {supported}"
                )
            
            loader_class = cls._EXTENSION_MAP[extension]
            loader_class.save(config, path)
