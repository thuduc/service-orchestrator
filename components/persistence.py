from typing import Dict, Any
from framework.base_component import BaseComponent
import json
import os


class PersistenceComponent(BaseComponent):
    """Component for data persistence"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Persistence component
        
        Args:
            config: Optional configuration parameters
        """
        super().__init__(config)
        self.output_dir = self.config.get('output_dir', './output')
        self.format = self.config.get('format', 'json')
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Persist processed data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Updated context with persistence results
        """
        # Set up logger by calling parent's execute
        super().execute(context)
        
        self.log_info("Starting data persistence")
        
        # Get data to persist - prefer 'processed' key, fallback to full context
        if 'processed' in context:
            data_to_persist = context['processed']
            self.log_info("Persisting 'processed' data")
        elif 'transformed_data' in context:
            data_to_persist = context['transformed_data']
            self.log_info("Persisting 'transformed_data'")
        else:
            # Persist the entire context excluding internal keys
            data_to_persist = {k: v for k, v in context.items() 
                             if not k.startswith('_')}
            self.log_info("Persisting full context (excluding internal keys)")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        self.log_debug(f"Output directory: {self.output_dir}")
        
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
            
            # Add persistence results to context
            context['persisted'] = True
            context['filepath'] = filepath
            context['size'] = os.path.getsize(filepath)
            context['persist_format'] = self.format
            
            self.log_info(f"Successfully persisted data to {filepath} ({context['size']} bytes)")
            
        except Exception as e:
            context['persisted'] = False
            context['persist_error'] = str(e)
            context['persist_format'] = self.format
            self.log_error(f"Failed to persist data: {str(e)}")
        
        return context