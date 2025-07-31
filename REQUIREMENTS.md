
Here are the requirements for a new model application that we're building:
- The code will be built using Python
- The application is made up of many components
- All components will implement the same base contract, with a single method: 
  > def execute(self, context) -> dict
  > context is also a dictionary in order to be flexible
- Instead of designing each component as a microservice, we would like to simplify and have a modular design where we will have 1 single service entrypoint
- This service entrypoint will run as a microservice. It also has one single method:
  > def execute(self, context) -> dict
  > context is also a dictionary in order to be flexible
- The service entrypoint will lookup the ServiceId using the context dictionary. Using the value of the ServiceId, the service entrypoint will loook up the name of the Python module using a json configuration file. The value of the python module will allow the service entrypoing to instance a new component (remember that all components implement the same contract), then invoke execute(self, context)
- As a software architect, I want you to design a framework for the above requirements. The framework will allow me to add and register new components easily.
- Think of other design patterns that can be added so the framework can support additional requirements and other cross-cutting converns in the future. For example: logging, input validation, output validation, additional behaviors before or after the component is run by the service entrypoint, etc.




> Use the @requirements.md to come up with a modular design for this framework. Save the design as FRAMEWORK_DESIGN.md

> Go ahead and do the initial implementation of this framework, along with 2 example components: Pre-Calibration Component, and Simulation Component. The implementation of each component execute method will just say "Hello World from {ComponentName}"