"""Synthetic data generator for testing data quality monitoring."""

import time
import random
import yaml
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import string
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, TIMESTAMP, text
from sqlalchemy.schema import CreateTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataGenerator:
    """Generates synthetic data according to specified patterns."""

    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        # Setup database connection
        db_config = self.config['database']
        conn_str = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        self.engine = create_engine(conn_str)
        self.metadata = MetaData()

    def setup_tables(self):
        """Create tables if they don't exist."""
        for table_config in self.config['tables']:
            columns = []
            for col in table_config['columns']:
                col_type = self._get_sql_type(col['type'])
                if isinstance(col_type, tuple):
                    type_class, type_kwargs = col_type
                    columns.append(Column(col['name'], type_class, **type_kwargs))
                else:
                    columns.append(Column(col['name'], col_type))

            Table(table_config['name'], self.metadata, *columns)

        self.metadata.create_all(self.engine)

    def _get_sql_type(self, type_str: str):
        """Convert string type definition to SQLAlchemy type."""
        if 'SERIAL' in type_str:
            return Integer, {'primary_key': True, 'autoincrement': True}
        elif type_str == 'FLOAT':
            return Float
        elif type_str.startswith('VARCHAR'):
            size = int(type_str.split('(')[1].split(')')[0])
            return String(size)
        elif type_str == 'TIMESTAMP':
            return TIMESTAMP
        raise ValueError(f"Unsupported type: {type_str}")

    def _generate_value(self, type_str: str) -> str:
        """Generate a random value based on column type."""
        if type_str == 'FLOAT':
            return random.uniform(0, 1000)
        elif type_str.startswith('VARCHAR'):
            size = min(int(type_str.split('(')[1].split(')')[0]), 10)
            return ''.join(random.choices(string.ascii_letters, k=size))
        elif type_str == 'TIMESTAMP':
            return datetime.now()
        return None

    def truncate_tables(self):
        """Truncate all tables before generating new data."""
        with self.engine.begin() as conn:
            for table_config in self.config['tables']:
                try:
                    table_name = table_config['name']
                    conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                    logger.info(f"Truncated table {table_name}")
                except Exception as e:
                    logger.error(f"Error truncating table {table_name}: {str(e)}")

    def generate_data(self, days: int = 7):
        """Generate historical data for the specified number of days."""
        # Truncate all tables first
        self.truncate_tables()
        # Use fixed end time at midnight
        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=days)

        for table_config in self.config['tables']:
            pattern = table_config['update_pattern']
            records = []

            # Pre-calculate all timestamps based on pattern
            timestamps = []
            current = start_time
            while current <= end_time:
                if pattern == 'realtime':
                    # Every 15 minutes
                    if current.minute % 15 == 0:
                        timestamps.append(current)
                    current += timedelta(minutes=15)
                elif pattern == 'hourly':
                    if current.minute == 0:
                        timestamps.append(current)
                    current += timedelta(hours=1)
                elif pattern == 'daily':
                    if current.hour == 0 and current.minute == 0:
                        timestamps.append(current)
                    current += timedelta(days=1)

            logger.info(f"Generating data for table {table_config['name']} with {len(timestamps)} timestamps")
            
            # Generate all records first
            records = []
            for ts in timestamps:
                num_records = random.randint(5, 20)
                for _ in range(num_records):
                    record = {}
                    for col in table_config['columns']:
                        if 'SERIAL' not in col['type']:  # Skip SERIAL columns
                            if col['name'] == 'updated_at':
                                record[col['name']] = ts
                            else:
                                record[col['name']] = self._generate_value(col['type'])
                    records.append(record)
            
            # Bulk insert all records at once
            if records:
                try:
                    with self.engine.begin() as conn:  # Auto-commits
                        table = self.metadata.tables[table_config['name']]
                        conn.execute(table.insert(), records)
                        logger.info(f"Inserted {len(records)} records into {table_config['name']}")
                except Exception as e:
                    logger.error(f"Error inserting into {table_config['name']}: {str(e)}")


def main():
    generator = DataGenerator('dqt/generator/config.yaml')
    generator.setup_tables()
    generator.generate_data(days=7)  # Generate 1 week of historical data

if __name__ == '__main__':
    main()
