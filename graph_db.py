"""
Graph Agent - Secondary Agent for Graph Database Queries
Interfaces with the existing shopfloor graph database tool

This agent:
1. Handles structured graph queries
2. Uses the existing ShopfloorGraphTool
3. Provides formatted responses for the main agent
4. Maps natural language to graph operations
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
import time
from dataclasses import dataclass
import re
import os

# Import the existing graph tool
from mcp_tool import ShopfloorGraphTool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GraphResponse:
    """Response from graph operations"""
    success: bool
    data: Dict[str, Any]
    execution_time: float
    operation_used: Optional[str] = None
    error_message: Optional[str] = None

class GraphAgent:
    """Secondary agent for graph database operations"""
    
    def __init__(self):
        self.graph_tool = None
        self.initialized = False
        self.stats = {
            'queries_processed': 0,
            'operations_used': {},
            'total_execution_time': 0,
            'average_execution_time': 0,
            'errors': 0
        }
        self.operation_mappings = self._setup_operation_mappings()
    
    def _setup_operation_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Setup mappings from natural language patterns to graph operations"""
        return {
            'highVibrationMachines': {
                'patterns': [
                    r'high vibration',
                    r'vibration.*machine',
                    r'machine.*vibration',
                    r'excessive vibration'
                ],
                'description': 'Find machines with high vibration readings',
                'required_params': ['threshold', 'unit']
            },
            'overdueWorkOrders': {
                'patterns': [
                    r'overdue.*work.*order',
                    r'work.*order.*overdue',
                    r'delayed.*maintenance',
                    r'pending.*work'
                ],
                'description': 'Find machines with overdue work orders',
                'required_params': []
            },
            'currentOperator': {
                'patterns': [
                    r'current.*operator',
                    r'operator.*machine',
                    r'who.*operating',
                    r'machine.*operator'
                ],
                'description': 'Get current operator for a machine',
                'required_params': ['machineId']
            },
            'sensorsByZone': {
                'patterns': [
                    r'sensor.*zone',
                    r'zone.*sensor',
                    r'monitoring.*zone',
                    r'sensors.*area'
                ],
                'description': 'List sensors in a specific zone',
                'required_params': ['zoneId']
            },
            'dueForMaintenance': {
                'patterns': [
                    r'maintenance.*due',
                    r'due.*maintenance',
                    r'maintenance.*schedule',
                    r'upcoming.*maintenance'
                ],
                'description': 'Find machines due for maintenance',
                'required_params': ['cutoff']
            },
            'listAllMachines': {
                'patterns': [
                    r'list.*machine',
                    r'all.*machine',
                    r'available.*machine',
                    r'machine.*available',
                    r'show.*machine',
                    r'what.*machine',
                    r'which.*machine'
                ],
                'description': 'List all available machines in the shopfloor',
                'required_params': []
            },
            'findQualifiedOperators': {
                'patterns': [
                    r'qualified.*operator',
                    r'operator.*certification',
                    r'certified.*operator',
                    r'find.*operator.*skill',
                    r'operator.*qualified',
                    r'maintenance.*operator'
                ],
                'description': 'Find operators with specific certifications',
                'required_params': ['certification']
            },
            'workOrdersByStatus': {
                'patterns': [
                    r'work.*order.*status',
                    r'status.*work.*order',
                    r'in.*progress.*work',
                    r'scheduled.*work',
                    r'work.*order.*overdue'
                ],
                'description': 'Find work orders by status',
                'required_params': ['status']
            },
            'maintenanceHistory': {
                'patterns': [
                    r'maintenance.*history',
                    r'history.*maintenance',
                    r'past.*maintenance',
                    r'maintenance.*log',
                    r'repair.*history'
                ],
                'description': 'Get maintenance history for a machine',
                'required_params': ['machineId']
            },
            'machineCurrentSensors': {
                'patterns': [
                    r'sensor.*reading',
                    r'current.*sensor',
                    r'machine.*sensor',
                    r'sensor.*data',
                    r'monitoring.*data'
                ],
                'description': 'Get current sensor readings for a machine',
                'required_params': ['machineId']
            },
            'currentProductionBatches': {
                'patterns': [
                    r'production.*batch',
                    r'current.*batch',
                    r'active.*batch',
                    r'running.*batch',
                    r'batch.*production'
                ],
                'description': 'Find currently active production batches',
                'required_params': []
            },
            'machinesInZone': {
                'patterns': [
                    r'machine.*zone',
                    r'zone.*machine',
                    r'machine.*area',
                    r'area.*machine',
                    r'location.*machine'
                ],
                'description': 'List machines in a specific zone',
                'required_params': ['zoneId']
            },
            'operatorsByShift': {
                'patterns': [
                    r'operator.*shift',
                    r'shift.*operator',
                    r'day.*operator',
                    r'night.*operator',
                    r'shift.*schedule'
                ],
                'description': 'List operators by shift',
                'required_params': ['shift']
            },
            'temperatureAlerts': {
                'patterns': [
                    r'temperature.*alert',
                    r'high.*temperature',
                    r'temperature.*warning',
                    r'overheating',
                    r'hot.*machine'
                ],
                'description': 'Find machines with high temperature',
                'required_params': ['threshold']
            }
        }
    
    async def initialize(self):
        """Initialize the graph agent"""
        try:
            # Get Neo4j credentials from environment
            uri = os.environ.get('NEO4J_URI')
            user = os.environ.get('NEO4J_USER')
            password = os.environ.get('NEO4J_PASSWORD')
            
            if not all([uri, user, password]):
                raise ValueError('NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD environment variables must be set')
            
            # Initialize graph tool
            self.graph_tool = ShopfloorGraphTool(uri, user, password)
            
            # Test connection with a simple query
            test_result = await self._execute_with_timeout(
                lambda: self.graph_tool.call('overdueWorkOrders', {}),
                timeout=10
            )
            
            self.initialized = True
            logger.info("Graph agent initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing graph agent: {e}")
            self.initialized = False
            return False
    
    async def _execute_with_timeout(self, func, timeout: int = 15):
        """Execute a function with timeout"""
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, func),
            timeout=timeout
        )
    
    def _extract_parameters(self, query: str, operation: str) -> Dict[str, Any]:
        """Extract parameters from natural language query"""
        params = {}
        query_lower = query.lower()
        
        if operation == 'highVibrationMachines':
            # Extract threshold and unit
            # Look for patterns like "7.5 mm/s", "threshold 8", "above 6.0"
            threshold_patterns = [
                r'(\d+\.?\d*)\s*(mm/s|m/s)',
                r'threshold\s*(\d+\.?\d*)',
                r'above\s*(\d+\.?\d*)',
                r'over\s*(\d+\.?\d*)',
                r'exceeds?\s*(\d+\.?\d*)'
            ]
            
            for pattern in threshold_patterns:
                match = re.search(pattern, query_lower)
                if match:
                    params['threshold'] = float(match.group(1))
                    if len(match.groups()) > 1:
                        params['unit'] = match.group(2)
                    break
            
            # Default values if not found
            if 'threshold' not in params:
                params['threshold'] = 7.5  # Default threshold
            if 'unit' not in params:
                params['unit'] = 'mm/s'  # Default unit
        
        elif operation == 'currentOperator':
            # Extract machine ID
            machine_patterns = [
                r'machine\s*([A-Z]\d+)',
                r'([A-Z]\d+)',
                r'machine\s*id\s*([A-Z]\d+)'
            ]
            
            for pattern in machine_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    params['machineId'] = match.group(1).upper()
                    break
            
            # Default machine if not found
            if 'machineId' not in params:
                params['machineId'] = 'M100'  # Default machine
        
        elif operation == 'sensorsByZone':
            # Extract zone ID
            zone_patterns = [
                r'zone\s*([A-Z]\d+)',
                r'([A-Z]\d+)',
                r'area\s*([A-Z]\d+)'
            ]
            
            for pattern in zone_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    params['zoneId'] = match.group(1).upper()
                    break
            
            # Default zone if not found
            if 'zoneId' not in params:
                params['zoneId'] = 'Z1'  # Default zone
        
        elif operation == 'dueForMaintenance':
            # Extract or use default cutoff date
            from datetime import datetime, timedelta
            
            # Look for date patterns
            date_patterns = [
                r'before\s*(\d{4}-\d{2}-\d{2})',
                r'since\s*(\d{4}-\d{2}-\d{2})',
                r'cutoff\s*(\d{4}-\d{2}-\d{2})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, query_lower)
                if match:
                    params['cutoff'] = match.group(1) + 'T00:00:00Z'
                    break
            
            # Default to 30 days ago if not found
            if 'cutoff' not in params:
                cutoff_date = datetime.now() - timedelta(days=30)
                params['cutoff'] = cutoff_date.strftime('%Y-%m-%dT00:00:00Z')
        
        return params
    
    def _determine_operation(self, query: str) -> Optional[str]:
        """Determine which graph operation to use based on query"""
        query_lower = query.lower()
        
        for operation, config in self.operation_mappings.items():
            for pattern in config['patterns']:
                if re.search(pattern, query_lower):
                    return operation
        
        return None
    
    async def execute_operation(self, operation: str, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a specific graph operation"""
        start_time = time.time()
        
        try:
            if not self.initialized:
                await self.initialize()
            
            if not self.initialized:
                return {
                    'success': False,
                    'error': 'Graph agent not initialized',
                    'operation': operation
                }
            
            # Extract parameters if not provided
            if params is None:
                params = self._extract_parameters(query, operation)
            
            # Execute the operation
            result = await self._execute_with_timeout(
                lambda: self.graph_tool.call(operation, params)
            )
            
            execution_time = time.time() - start_time
            self._update_stats(operation, execution_time)
            
            return {
                'success': True,
                'operation': operation,
                'parameters': params,
                'data': result,
                'count': len(result) if isinstance(result, list) else 1,
                'execution_time': execution_time
            }
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error executing operation {operation}: {e}")
            return {
                'success': False,
                'error': str(e),
                'operation': operation,
                'execution_time': time.time() - start_time
            }
    
    async def query_general(self, query: str) -> Dict[str, Any]:
        """Handle general queries by determining the best operation"""
        start_time = time.time()
        
        try:
            # Determine operation
            operation = self._determine_operation(query)
            
            if operation:
                return await self.execute_operation(operation, query)
            else:
                # Try to suggest available operations
                suggestions = self._get_operation_suggestions(query)
                
                return {
                    'success': False,
                    'error': 'No matching graph operation found',
                    'query': query,
                    'suggestions': suggestions,
                    'available_operations': list(self.operation_mappings.keys()),
                    'execution_time': time.time() - start_time
                }
        
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error in general query: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query,
                'execution_time': time.time() - start_time
            }
    
    def _get_operation_suggestions(self, query: str) -> List[str]:
        """Get operation suggestions based on query keywords"""
        suggestions = []
        query_lower = query.lower()
        
        keywords = {
            'machine': ['highVibrationMachines', 'currentOperator', 'dueForMaintenance'],
            'vibration': ['highVibrationMachines'],
            'operator': ['currentOperator'],
            'sensor': ['sensorsByZone'],
            'zone': ['sensorsByZone'],
            'maintenance': ['dueForMaintenance'],
            'work order': ['overdueWorkOrders'],
            'overdue': ['overdueWorkOrders', 'dueForMaintenance']
        }
        
        for keyword, operations in keywords.items():
            if keyword in query_lower:
                suggestions.extend(operations)
        
        return list(set(suggestions))  # Remove duplicates
    
    def _update_stats(self, operation: str, execution_time: float):
        """Update agent statistics"""
        self.stats['queries_processed'] += 1
        self.stats['operations_used'][operation] = self.stats['operations_used'].get(operation, 0) + 1
        self.stats['total_execution_time'] += execution_time
        self.stats['average_execution_time'] = (
            self.stats['total_execution_time'] / self.stats['queries_processed']
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get agent status for main agent"""
        return {
            'active': self.initialized,
            'type': 'GraphAgent',
            'graph_tool_connected': self.graph_tool is not None,
            'stats': self.stats,
            'available_operations': list(self.operation_mappings.keys())
        }
    
    async def find_high_vibration_machines(self, threshold: float = 7.5, unit: str = "mm/s") -> Dict[str, Any]:
        """Find machines with high vibration readings"""
        try:
            result = await self.execute_operation(
                operation='highVibrationMachines',
                query=f'Find machines with high vibration above {threshold} {unit}',
                params={'threshold': threshold, 'unit': unit}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to find high vibration machines: {str(e)}',
                'data': []
            }
    
    async def get_overdue_work_orders(self) -> Dict[str, Any]:
        """Get overdue work orders"""
        try:
            result = await self.execute_operation(
                operation='overdueWorkOrders',
                query='Find overdue work orders',
                params={}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get overdue work orders: {str(e)}',
                'data': []
            }
    
    async def get_current_operator(self, machine_id: str) -> Dict[str, Any]:
        """Get current operator for a machine"""
        try:
            if not machine_id:
                return {
                    'success': False,
                    'error': 'machineId parameter is required',
                    'data': []
                }
            result = await self.execute_operation(
                operation='currentOperator',
                query=f'Who is the current operator for machine {machine_id}',
                params={'machineId': machine_id}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get current operator: {str(e)}',
                'data': []
            }
    
    async def get_sensors_by_zone(self, zone_id: str) -> Dict[str, Any]:
        """Get sensors in a specific zone"""
        try:
            result = await self.execute_operation(
                operation='sensorsByZone',
                query=f'List sensors in zone {zone_id}',
                params={'zone_id': zone_id}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get sensors by zone: {str(e)}',
                'data': []
            }
    
    async def get_machines_due_maintenance(self, cutoff_date: str = None) -> Dict[str, Any]:
        """Get machines due for maintenance
        
        Args:
            cutoff_date: ISO date string (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)
                        Finds machines with lastMaintenance before this date
                        Defaults to 30 days ago if not provided
        """
        try:
            from datetime import datetime, timedelta
            
            # If no cutoff provided, default to 30 days ago
            if not cutoff_date:
                cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT00:00:00Z')
            # If date format is YYYY-MM-DD, convert to ISO datetime
            elif 'T' not in cutoff_date:
                cutoff_date = f"{cutoff_date}T00:00:00Z"
            
            result = await self.execute_operation(
                operation='dueForMaintenance',
                query=f'Find machines due for maintenance before {cutoff_date}',
                params={'cutoff': cutoff_date}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get machines due maintenance: {str(e)}',
                'data': []
            }
    
    async def list_all_machines(self) -> Dict[str, Any]:
        """List all available machines in the shopfloor"""
        try:
            result = await self.execute_operation(
                operation='listAllMachines',
                query='List all available machines',
                params={}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to list all machines: {str(e)}',
                'data': []
            }
    
    async def find_qualified_operators(self, certification: str) -> Dict[str, Any]:
        """Find operators with specific certifications"""
        try:
            result = await self.execute_operation(
                operation='findQualifiedOperators',
                query=f'Find operators qualified in {certification}',
                params={'certification': certification}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to find qualified operators: {str(e)}',
                'data': []
            }
    
    async def get_work_orders_by_status(self, status: str) -> Dict[str, Any]:
        """Get work orders by status"""
        try:
            result = await self.execute_operation(
                operation='workOrdersByStatus',
                query=f'Find work orders with status {status}',
                params={'status': status}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get work orders by status: {str(e)}',
                'data': []
            }
    
    async def get_maintenance_history(self, machine_id: str) -> Dict[str, Any]:
        """Get maintenance history for a machine"""
        try:
            result = await self.execute_operation(
                operation='maintenanceHistory',
                query=f'Get maintenance history for machine {machine_id}',
                params={'machineId': machine_id}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get maintenance history: {str(e)}',
                'data': []
            }
    
    async def get_machine_current_sensors(self, machine_id: str) -> Dict[str, Any]:
        """Get current sensor readings for a machine"""
        try:
            result = await self.execute_operation(
                operation='machineCurrentSensors',
                query=f'Get current sensors for machine {machine_id}',
                params={'machineId': machine_id}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get machine sensors: {str(e)}',
                'data': []
            }
    
    async def get_current_production_batches(self) -> Dict[str, Any]:
        """Get currently active production batches"""
        try:
            result = await self.execute_operation(
                operation='currentProductionBatches',
                query='Find current production batches',
                params={}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get production batches: {str(e)}',
                'data': []
            }
    
    async def get_machines_in_zone(self, zone_id: str) -> Dict[str, Any]:
        """Get machines in a specific zone"""
        try:
            result = await self.execute_operation(
                operation='machinesInZone',
                query=f'Get machines in zone {zone_id}',
                params={'zoneId': zone_id}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get machines in zone: {str(e)}',
                'data': []
            }
    
    async def get_operators_by_shift(self, shift: str) -> Dict[str, Any]:
        """Get operators by shift"""
        try:
            result = await self.execute_operation(
                operation='operatorsByShift',
                query=f'Get operators working {shift} shift',
                params={'shift': shift}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get operators by shift: {str(e)}',
                'data': []
            }
    
    async def get_temperature_alerts(self, threshold: float = 60.0) -> Dict[str, Any]:
        """Get machines with high temperature readings"""
        try:
            result = await self.execute_operation(
                operation='temperatureAlerts',
                query=f'Find machines with temperature above {threshold}',
                params={'threshold': threshold}
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to get temperature alerts: {str(e)}',
                'data': []
            }
    
    async def get_available_operations(self) -> Dict[str, Any]:
        """Get list of available graph operations"""
        return {
            'operations': {
                name: {
                    'description': config['description'],
                    'required_params': config['required_params'],
                    'patterns': config['patterns']
                }
                for name, config in self.operation_mappings.items()
            },
            'total_operations': len(self.operation_mappings)
        }
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test graph database connection"""
        try:
            if not self.initialized:
                await self.initialize()
            
            # Test with simple query
            result = await self._execute_with_timeout(
                lambda: self.graph_tool.call('overdueWorkOrders', {}),
                timeout=5
            )
            
            return {
                'success': True,
                'message': 'Graph database connection successful',
                'test_query': 'overdueWorkOrders',
                'result_count': len(result) if isinstance(result, list) else 1
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Graph database connection failed: {str(e)}'
            }
    
    def cleanup(self):
        """Cleanup resources"""
        if self.graph_tool:
            self.graph_tool.close()
            self.initialized = False

# Testing and example usage
async def test_graph_agent():
    """Test the graph agent functionality"""
    agent = GraphAgent()
    
    try:
        # Test initialization
        print("Initializing graph agent...")
        success = await agent.initialize()
        if not success:
            print("Failed to initialize graph agent")
            return
        
        # Test connection
        print("\nTesting database connection...")
        conn_result = await agent.test_connection()
        print(f"Connection success: {conn_result['success']}")
        if not conn_result['success']:
            print(f"Connection error: {conn_result.get('error')}")
            return
        
        # Test specific operations
        test_queries = [
            "Show machines with high vibration above 7.5 mm/s",
            "What work orders are overdue?",
            "Who is the current operator for machine M100?",
            "List sensors in zone Z1",
            "Which machines are due for maintenance?"
        ]
        
        for query in test_queries:
            print(f"\n{'='*50}")
            print(f"Query: {query}")
            print('='*50)
            
            result = await agent.query_general(query)
            print(f"Success: {result['success']}")
            
            if result['success']:
                print(f"Operation: {result.get('operation')}")
                print(f"Parameters: {result.get('parameters')}")
                print(f"Results: {result.get('count', 0)} records")
                print(f"Execution time: {result.get('execution_time', 0):.3f}s")
                
                # Show first few results
                data = result.get('data', [])
                if isinstance(data, list) and data:
                    print("Sample results:")
                    for i, record in enumerate(data[:2], 1):
                        print(f"  {i}. {record}")
            else:
                print(f"Error: {result.get('error')}")
                if 'suggestions' in result:
                    print(f"Suggestions: {result['suggestions']}")
        
        # Show available operations
        print(f"\n{'='*50}")
        print("Available Operations")
        print('='*50)
        operations = await agent.get_available_operations()
        for name, info in operations['operations'].items():
            print(f"{name}: {info['description']}")
        
        # Show agent status
        print(f"\n{'='*50}")
        print("Agent Status")
        print('='*50)
        status = agent.get_status()
        print(json.dumps(status, indent=2))
        
    except Exception as e:
        print(f"Test error: {e}")
    
    finally:
        agent.cleanup()

async def main():
    """Main entry point for testing"""
    await test_graph_agent()

if __name__ == "__main__":
    asyncio.run(main())