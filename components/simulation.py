from typing import Dict, Any
from framework.base_component import BaseComponent


class SimulationComponent(BaseComponent):
    """Component for simulation processing"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Simulation component
        
        Args:
            config: Optional configuration parameters
        """
        super().__init__(config)
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the simulation logic
        
        Args:
            context: Input context dictionary
            
        Returns:
            Dictionary with execution results
        """
        # Set up logger by calling parent's execute
        super().execute(context)
        
        self.log_info('Hi from Simulation Component')
        return {
            "status": "success",
            "message": "Hello World from Simulation Component",
            "service_id": context.get("service_id", "simulation"),
            "request_id": context.get("request_id"),
            "component_type": "SimulationComponent"
        }