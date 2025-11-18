"""
Load CSV data into Neo4j database
This script reads all CSV files from the data/ folder and imports them into Neo4j
"""

import os
import pandas as pd
from neo4j import GraphDatabase
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jDataLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.data_dir = Path("data")
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            logger.info("🗑️  Clearing existing data...")
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("✅ Database cleared")
    
    def load_nodes(self):
        """Load all node CSV files"""
        node_files = {
            'Machine': 'nodes_machines.csv',
            'Operator': 'nodes_operators.csv',
            'Sensor': 'nodes_sensors.csv',
            'WorkOrder': 'nodes_workorders.csv',
            'MaintenanceLog': 'nodes_maintenancelogs.csv',
            'FacilityZone': 'nodes_facilityzones.csv',
            'ProductionBatch': 'nodes_productionbatches.csv'
        }
        
        for label, filename in node_files.items():
            filepath = self.data_dir / filename
            if not filepath.exists():
                logger.warning(f"⚠️  File not found: {filepath}")
                continue
            
            df = pd.read_csv(filepath)
            logger.info(f"📊 Loading {len(df)} {label} nodes from {filename}...")
            
            with self.driver.session() as session:
                for _, row in df.iterrows():
                    # Convert row to properties dict, handling NaN values
                    props = {}
                    for col, val in row.items():
                        if pd.notna(val):
                            props[col] = val
                    
                    # Create Cypher query
                    query = f"CREATE (n:{label} $props)"
                    session.run(query, props=props)
            
            logger.info(f"✅ Loaded {len(df)} {label} nodes")
    
    def load_relationships(self):
        """Load all relationship CSV files"""
        rel_files = {
            'HAS_LOG': 'rels_has_log.csv',
            'HAS_WORK_ORDER': 'rels_has_work_order.csv',
            'LOCATED_IN': 'rels_located_in.csv',
            'MONITORED_BY': 'rels_monitored_by.csv',
            'OPERATED_BY': 'rels_operated_by.csv',
            'PART_OF_BATCH': 'rels_part_of_batch.csv'
        }
        
        for rel_type, filename in rel_files.items():
            filepath = self.data_dir / filename
            if not filepath.exists():
                logger.warning(f"⚠️  File not found: {filepath}")
                continue
            
            df = pd.read_csv(filepath)
            logger.info(f"🔗 Loading {len(df)} {rel_type} relationships from {filename}...")
            
            with self.driver.session() as session:
                for _, row in df.iterrows():
                    # Handle both naming conventions: fromId/toId or sourceId/targetId
                    from_id = row.get('fromId') or row.get('sourceId')
                    to_id = row.get('toId') or row.get('targetId')
                    
                    if not from_id or not to_id:
                        logger.warning(f"⚠️  Missing source/target IDs in row: {row}")
                        continue
                    
                    # Get relationship properties (all columns except ID columns)
                    rel_props = {}
                    for col, val in row.items():
                        if col not in ['fromId', 'toId', 'sourceId', 'targetId'] and pd.notna(val):
                            rel_props[col] = val
                    
                    # Create relationship - match nodes by their 'id' property
                    query = f"""
                    MATCH (from {{id: $fromId}}), (to {{id: $toId}})
                    CREATE (from)-[r:{rel_type} $props]->(to)
                    """
                    
                    try:
                        result = session.run(query, fromId=str(from_id), toId=str(to_id), props=rel_props)
                        summary = result.consume()
                        if summary.counters.relationships_created == 0:
                            logger.warning(f"⚠️  No relationship created for {from_id} -> {to_id}")
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to create {rel_type}: {from_id} -> {to_id} - {e}")
            
            logger.info(f"✅ Loaded {len(df)} {rel_type} relationships")
    
    def verify_load(self):
        """Verify data was loaded correctly"""
        with self.driver.session() as session:
            # Count nodes
            result = session.run("MATCH (n) RETURN count(n) as count")
            node_count = result.single()["count"]
            
            # Count relationships
            result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            rel_count = result.single()["count"]
            
            logger.info(f"📊 Database Summary:")
            logger.info(f"   Nodes: {node_count}")
            logger.info(f"   Relationships: {rel_count}")
            
            # Count by label
            result = session.run("MATCH (n) RETURN labels(n) as label, count(n) as count")
            logger.info(f"   Node Types:")
            for record in result:
                logger.info(f"      {record['label'][0]}: {record['count']}")
            
            # Count by relationship type
            result = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count")
            logger.info(f"   Relationship Types:")
            for record in result:
                logger.info(f"      {record['type']}: {record['count']}")
            
            return node_count > 0 and rel_count > 0

def main():
    # Get Neo4j credentials from environment
    uri = os.environ.get('NEO4J_URI')
    user = os.environ.get('NEO4J_USERNAME') or os.environ.get('NEO4J_USER')
    password = os.environ.get('NEO4J_PASSWORD')
    
    if not all([uri, user, password]):
        logger.error('Error: NEO4J_URI, NEO4J_USER/NEO4J_USERNAME, and NEO4J_PASSWORD environment variables must be set')
        sys.exit(1)
    
    logger.info("🚀 Starting Neo4j data import...")
    logger.info(f"   URI: {uri}")
    logger.info(f"   User: {user}")
    
    loader = Neo4jDataLoader(uri, user, password)
    
    try:
        # Optional: Clear existing data (CAUTION!)
        response = input("⚠️  Clear existing database? (yes/no): ")
        if response.lower() == 'yes':
            loader.clear_database()
        
        # Load nodes first
        loader.load_nodes()
        
        # Then load relationships
        loader.load_relationships()
        
        # Verify
        success = loader.verify_load()
        
        if success:
            logger.info("🎉 Data import completed successfully!")
        else:
            logger.error("❌ Data import may have failed - check logs")
    
    except Exception as e:
        logger.error(f"❌ Import failed: {e}")
    
    finally:
        loader.close()

if __name__ == "__main__":
    main()
