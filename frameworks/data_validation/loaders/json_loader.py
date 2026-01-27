"""JSON configuration loader."""

import json
from pathlib import Path
from typing import Any, Dict, Union


class JsonLoader:
    """
    Loads configuration from JSON files.
    
    Supports loading both pipeline configurations and custom check definitions
    from JSON format files.
    """

    @staticmethod
    def load(path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a JSON file.
        
        Args:
            path: Path to the JSON configuration file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def load_string(content: str) -> Dict[str, Any]:
        """
        Load configuration from a JSON string.
        
        Args:
            content: JSON string to parse
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            json.JSONDecodeError: If the string contains invalid JSON
        """
        return json.loads(content)

    @staticmethod
    def save(config: Dict[str, Any], path: Union[str, Path], indent: int = 2) -> None:
        """
        Save configuration to a JSON file.
        
        Args:
            config: Configuration dictionary to save
            path: Path to save the configuration to
            indent: JSON indentation level (default: 2)
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=indent)
