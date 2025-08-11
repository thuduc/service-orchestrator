from typing import Dict, Any
from framework.component import Component
import json
import os


class PersistenceComponent(Component):
    """Component for data persistence"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Persistence component
        
        Args:
            config: Optional configuration parameters
        """
        self.config = config or {}
        self.output_dir = self.config.get('output_dir', './output')
        self.format = self.config.get('format', 'json')
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Persist processed data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Dictionary with persistence results
        """
        print('Hi from Persistence Component')
        # Get data to persist
        data_to_persist = context.get('processed', context)
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Generate filename
        request_id = context.get('request_id', 'unknown')
        filename = f"{request_id}_result.{self.format}"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            # Save data
            if self.format == 'json':
                with open(filepath, 'w') as f:
                    json.dump(data_to_persist, f, indent=2)
            else:
                with open(filepath, 'w') as f:
                    f.write(str(data_to_persist))
            
            return {
                "persisted": True,
                "filepath": filepath,
                "size": os.path.getsize(filepath),
                "format": self.format
            }
        except Exception as e:
            return {
                "persisted": False,
                "error": str(e),
                "format": self.format
            }