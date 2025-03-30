from typing import Dict, Any
from datetime import datetime
import pandas as pd
from repository.duckdb_repo import DuckDBRepository
from config import config
import json
from pyspark.sql import SparkSession

import asyncio
from concurrent.futures import ThreadPoolExecutor

class MetricsService:
    """Service for collecting and analyzing business metrics."""
    
    def __init__(self):
        try:
            self.repo = DuckDBRepository(
                db_path=config['database']['duckdb_path'],
                catalog=config['database']['catalog'],
                schema=config['database']['namespace']
            )
            
            # Initialize Spark session from config
            self.spark = self._create_spark_session()
        except Exception as e:
            print(f"Failed to initialize connections: {e}")
            raise

    def _create_spark_session(self):
        """Create a Spark session using configuration from config.yaml"""
        try:
            spark_config = config.get('spark', {})
            
            # Build the Spark session with configurations from yaml
            spark_builder = SparkSession.builder \
                .appName(spark_config.get('app_name', 'IcebreakerApp'))
                
            # Add all the configurations
            spark_builder = spark_builder \
                .config("spark.sql.extensions", spark_config.get('extensions')) \
                .config("spark.jars.packages", spark_config.get('packages')) \
                .config(f"spark.sql.catalog.{spark_config.get('catalog_name')}", "org.apache.iceberg.spark.SparkCatalog") \
                .config(f"spark.sql.catalog.{spark_config.get('catalog_name')}.type", spark_config.get('catalog_type')) \
                .config(f"spark.sql.catalog.{spark_config.get('catalog_name')}.warehouse", spark_config.get('warehouse')) \
                .config("spark.hadoop.fs.s3a.access.key", spark_config.get('s3a_access_key')) \
                .config("spark.hadoop.fs.s3a.secret.key", spark_config.get('s3a_secret_key')) \
                .config("spark.hadoop.fs.s3a.endpoint", spark_config.get('s3a_endpoint')) \
                .config("spark.driver.memory", spark_config.get('driver_memory')) \
                .config("spark.executor.memory", spark_config.get('executor_memory'))
            
            # Create and return the Spark session
            return spark_builder.getOrCreate()
            
        except Exception as e:
            print(f"Failed to initialize Spark session: {e}")
            return None

    def list_tables(self):
        """List all tables in the database."""
        try:
            result = self.repo.get_all_tables()
            return result.fetchdf() if result is not None else pd.DataFrame()
        except Exception as e:
            print(f"Error listing tables: {e}")
            return pd.DataFrame()

    def get_active_files_count(self, ) -> int:
        """Get count of active files from the repository."""
        try:
            active_files = self.repo.get_active_files()
            return len(active_files) if active_files is not None else 0
        except Exception as e:
            print(f"Error getting active files count: {e}")
            return 0
    
    def get_all_files_count(self, ) -> int:
        """Get count of all files from the repository."""
        try:
            all_files = self.repo.get_all_files()
            return len(all_files) if all_files is not None else 0
        except Exception as e:
            print(f"Error getting all files count: {e}")
            return 0

    def get_namespace_statistics(self, table_name=None):
        """Get namespace statistics in a structured format.
        
        Returns:
            dict: Contains namespace metrics including counts, sizes, and timestamp
        """
        try:
            # Get all files metrics
            all_files = self.repo.get_all_files()
            all_file_metrics = all_files.aggregate(
                'count(1) as total_files, sum(file_size_in_bytes)/1048576 as total_size_mb, sum(record_count)'
            ).fetchdf()
            
            # Get active files metrics
            active_files = self.repo.get_active_files()
            active_file_metrics = active_files.aggregate(
                'count(1) as active_files, sum(file_size_in_bytes)/1048576 as active_size_mb, sum(record_count)'
            ).fetchdf()
            
            # Get last updated timestamp
            history = self.repo.get_history()
            last_updated = history.aggregate('max(made_current_at) as last_updated').fetchdf()
            
            # Get total tables count
            tables = self.repo.get_all_tables()
            total_tables = len(tables) if tables is not None else 0
            print({
                'total_tables': total_tables,
                'total_files': int(all_file_metrics['total_files'].iloc[0]),
                'total_size_mb': float(all_file_metrics['total_size_mb'].iloc[0]),
                'last_updated': last_updated['last_updated'].iloc[0],
                'active_files': int(active_file_metrics['active_files'].iloc[0]),
                'active_size_mb': float(active_file_metrics['active_size_mb'].iloc[0])
            })

            return {
                'total_tables': total_tables,
                'total_files': int(all_file_metrics['total_files'].iloc[0]),
                'total_file_size': float(all_file_metrics['total_size_mb'].iloc[0]),
                'last_updated': last_updated['last_updated'].iloc[0],
                'total_active_files': int(active_file_metrics['active_files'].iloc[0]),
                'total_active_file_size': float(active_file_metrics['active_size_mb'].iloc[0])
            }

        except Exception as e:
            print(f"Error getting namespace statistics: {e}")
            return {
                'total_tables': 0,
                'total_files': 0,
                'total_size_mb': 0.0,
                'last_updated': None,
                'active_files': 0,
                'active_size_mb': 0.0
            }
    

    def get_all_table_metrics(self):
        """Get metrics for a specific table."""
        try:
            # Get all files for the table
            all_files = self.repo.get_all_files()

            all_file_metrics = all_files.aggregate(
                '''source_table, 
                count(1) as total_files, 
                sum(file_size_in_bytes)/1048576 as total_size_mb,
                sum(record_count) as all_total_records,
                avg(file_size_in_bytes)/1048576 as avg_file_size_mb'''
            ).set_alias("a")

            # Get active files for the table
            active_files = self.repo.get_active_files()


            active_file_metrics = active_files.aggregate(
                'source_table, count(1) as active_files, sum(file_size_in_bytes)/1048576 as active_size_mb, sum(record_count) as active_total_records'
            ).set_alias("b")

            active_file_metrics = active_file_metrics.join(all_file_metrics, 'a.source_table = b.source_table')

            table_metrics =(active_file_metrics.select('''a.source_table, b.active_files,a.total_files,
                                             b.active_size_mb,
                                            a.total_size_mb,
                                            a.all_total_records,
                                            b.active_total_records,
                                            a.avg_file_size_mb
                                            ''').fetchdf())
            
            return table_metrics
        

        except Exception as e:
            print(f"Error getting table metrics: {e}")
            return None 
        
    def get_table_snapshot_details(self, table_name):
        snapshot_details = self.repo.get_snapshot_details()

        snapshot_details = (snapshot_details.select('''source_table,
                                                    committed_at,
                                                    snapshot_id,
                                                    parent_id,
                                                    operation,
                                                    manifest_list,
                                                    summary['added-records'] as added_records,
                                                    summary['added-data-files'] as deleted_records,
                                                    summary['total-records'] as total_records,
                                                    summary['total-data-files'] as total_data_files,
                                                    summary['total-files-size'] as total_file_size
                                                    '''
                                                    )
                            .filter(f"source_table = '{table_name}'"))
        return snapshot_details.fetchdf()
        
    def get_table_growth_analysis(self, table_name):
        growth_metrics = self.repo.all_table_growth_metrics()
        growth_metrics = growth_metrics.filter(f"source_table = '{table_name}'").fetchdf()
        
        return growth_metrics
    
    def get_partition_details(self, table_name):
        partition_details = self.repo.get_partition_details()
        partition_details =(partition_details.select('''source_table, partition.partition_key as partition_key,
                                                        spec_id,
                                                        record_count as active_record_count,file_count as active_file_count,
                                                        total_data_file_size_in_bytes/1048576 as active_total_data_file_size_mb,
                                                        last_updated_at,last_updated_snapshot_id
                                                     '''
                                                     )
                                             .filter(f"source_table = '{table_name}'")).set_alias("a")
        
        all_data_files = self.repo.get_all_files()
        all_data_files = all_data_files.filter(f"source_table = '{table_name}'")
        all_data_files = all_data_files.aggregate('''partition.partition_key as partition_key,
                                                    source_table,
                                                    count(1) as total_files,
                                                    sum(file_size_in_bytes)/1048576 as total_size_mb,
                                                    sum(record_count) as all_total_records,
                                                    avg(file_size_in_bytes)/1048576 as avg_file_size_mb''').set_alias("b")
        

        partition_details = partition_details.join(all_data_files, 'a.partition_key = b.partition_key')
        
        partition_details = partition_details.select('''a.source_table, a.partition_key, a.spec_id, a.active_record_count, a.active_file_count,
                                                    a.active_total_data_file_size_mb, a.last_updated_at, a.last_updated_snapshot_id,
                                                    b.total_files, b.total_size_mb, b.all_total_records, b.avg_file_size_mb''')
        

        return partition_details.fetchdf()
    
    def get_partition_file_size(self, table_name):
        partition_details = self.repo.get_active_files()
        partition_details = partition_details.filter(f"source_table = '{table_name}'")
        partition_details = partition_details.aggregate('''partition.partition_key as partition_key,
                                                        file_path,
                                                        file_size_in_bytes/1048576 as file_size_mb''')
                                                        
        return partition_details.fetchdf()
        
    

    def read_metadata_json(self, path):
        """Read JSON metadata file using Spark
        
        Args:
            path: Path to the JSON file
            
        Returns:
            Dictionary containing the JSON data
        """
        try:
            # Read the JSON file using Spark
            df = self.spark.read.json(path, multiLine=True)
            df.show()
            
            # Convert to a single row pandas DataFrame and then to dict
            # This works for single JSON objects (not arrays of objects)
            pandas_df = df.toPandas()

            return pandas_df
        except Exception as e:
            print(f"Error reading JSON file with Spark: {e}")
            return None

    def get_table_schema(self, path):
        """
        Extract table schema from Iceberg metadata JSON file.
        
        Args:
            path (str): Path to the Iceberg metadata JSON file
            
        Returns:
            dict: Dictionary with keys col_0, col_1, etc. and values containing column details
        """
        try:
            # Read the metadata JSON file
            metadata_df = self.read_metadata_json(path)
            
            if metadata_df is None or metadata_df.empty:
                return {}
            
            schema_dict = {}
            
            # Extract schemas from the first row of the DataFrame
            if 'schemas' in metadata_df.columns and len(metadata_df['schemas']) > 0:
                # Get the first schema (assuming we want schema at index 0)
                schemas = metadata_df['schemas'].iloc[0]
                if len(schemas) > 0:
                    schema = schemas[0]  # First schema
                    
                    # Process fields
                    for idx, field in enumerate(schema['fields']):
                        col_key = f"{idx+1}"
                        
                        # Handle nested fields
                        if isinstance(field['type'], dict) and field['type'].get('type') == 'struct':
                            field_type = 'struct'
                        else:
                            field_type = field['type']
                            
                        schema_dict[col_key] = {
                            "col_name": field['name'],
                            "type": field_type,
                            "id": field['id'],
                            "required": field['required']
                        }
            
            return schema_dict
        except Exception as e:
            print(f"Error extracting schema from metadata: {e}")
            return {}

    def get_snapshot_details(self, snapshot_id):
        snapshot_details = self.repo.get_snapshot_details()
        snapshot_details = snapshot_details.filter(f"snapshot_id = '{snapshot_id}'")
        return snapshot_details.fetchdf()
    
    def get_table_metadata_path(self, table_name):
        
        query = f"select metadata_path from iceberg.information_schema.tables where table_name = '{table_name}'"
    

    def get_column_level_metrics(self, table_name):
        """
        Get column size metrics by joining schema information with column sizes from active files.
        
        Args:
            table_name (str): Name of the table to analyze
            schema_dict (dict): Dictionary containing schema information in the format 
                               {'1': {'col_name': 'id', 'type': 'string', 'id': 1, 'required': False}}
        
        Returns:
            pandas.DataFrame: DataFrame with column metrics including name, type, and size statistics
        """
        try:
            # Get active files for the table
            active_files = self.repo.get_active_files()
            active_files = active_files.filter(f"source_table = '{table_name}'")

            file_path = (self.repo.metadata_log_entries().filter(f"source_table = '{table_name}'")
                         .order("timestamp DESC")
                         .limit(1).fetchdf()['file'].iloc[0]
                         )



            schema_dict = self.get_table_schema(file_path)
            
            # Create a list to store column size data
            column_data = []
            
            # Process each file's column sizes
            files_df = active_files.fetchdf()
            
            if 'column_sizes' not in files_df.columns:
                print("Column sizes information not available in active files data")
                return pd.DataFrame()
            
            # Initialize dictionaries to track total size and count for each column
            total_sizes = {}
            column_counts = {}
            null_counts = {}
            
            # Process each file's column sizes
            for _, file_row in files_df.iterrows():
                if file_row['column_sizes'] is not None:
                    # Convert column_sizes from map to dictionary
                    # The format is typically {column_id: size_in_bytes}
                    column_sizes = file_row['column_sizes']
                    
                    # Process each column in this file
                    for col_id, size in column_sizes.items():
                        # Convert column ID to string to match schema_dict keys
                        col_id_str = str(col_id)
                        
                        # Update total size and count
                        if col_id_str in total_sizes:
                            total_sizes[col_id_str] += size
                            column_counts[col_id_str] += 1
                        else:
                            total_sizes[col_id_str] = size
                            column_counts[col_id_str] = 1
                
                # Process null value counts if available
                if 'null_value_counts' in files_df.columns and file_row['null_value_counts'] is not None:
                    null_value_counts = file_row['null_value_counts']
                    
                    # Process each column's null counts
                    for col_id, count in null_value_counts.items():
                        col_id_str = str(col_id)
                        
                        # Update null counts
                        if col_id_str in null_counts:
                            null_counts[col_id_str] += count
                        else:
                            null_counts[col_id_str] = count
            
            # Create the final dataset by joining with schema information
            for col_id, sizes in total_sizes.items():
                if col_id in schema_dict:
                    schema_info = schema_dict[col_id]
                    
                    column_data.append({
                        'column_id': col_id,
                        'column_name': schema_info['col_name'],
                        'data_type': schema_info['type'],
                        'required': schema_info['required'],
                        'total_size_bytes': sizes,
                        'total_size_mb': sizes / (1024 * 1024),
                        'null_count': null_counts.get(col_id, 0)
                    })
                else:
                    # Handle columns that exist in files but not in schema
                    column_data.append({
                        'column_id': col_id,
                        'column_name': f'unknown_column_{col_id}',
                        'data_type': 'unknown',
                        'required': False,
                        'total_size_bytes': sizes,
                        'total_size_mb': sizes / (1024 * 1024),
                        'null_count': null_counts.get(col_id, 0)
                    })
            
            # Convert to DataFrame and sort by total size
            result_df = pd.DataFrame(column_data)
            if not result_df.empty:
                result_df = result_df.sort_values('total_size_bytes', ascending=False)
            
            return result_df
        
        except Exception as e:
            print(f"Error getting column size metrics: {e}")
            return pd.DataFrame()
            
            
    def run_compaction_job(self, table_name: str) -> None:
        if table_name:
            # Extract table name if namespace is included
            if '.' in table_name:
                table_name = table_name.split('.')[-1]
        
            # Define the compaction SQL command
            compaction_query = f"""
            CALL {config['database']['catalog']}.system.rewrite_data_files(
                table => '{config['database']['namespace']}.{table_name}'
            )
            """
            
            try:
                # Execute the compaction job
                self.spark.sql(compaction_query)
                print(f"Compaction job successfully run on table {config['database']['namespace']}.{table_name}")
            except Exception as e:
                print(f"Error running compaction job: {str(e)}")

    def time_travel_to_snapshot(self, table_name: str, snapshot_id: str) -> None:
        """
        Time travel to a specific snapshot of an Iceberg table.
        
        Args:
            table_name (str): Name of the table
            snapshot_id (str): ID of the snapshot to travel to
            use_spark (bool): Whether to use Spark or DuckDB
            catalog (str): Catalog name
            namespace (str): Namespace name
        
        Returns:
            DataFrame: Result of the time travel query
        
        Raises:
            Exception: If time travel operation fails
        """
        try:
                query = f"CALL {config['database']['catalog']}.system.rollback_to_snapshot('{config['database']['namespace']}.{table_name}', {snapshot_id})";

                df = self.spark.sql(query)

                print(f"Time travel to snapshot {snapshot_id} successfully run on table {config['database']['namespace']}.{table_name}")

                self.copy_metrics_to_db()
                
        except Exception as e:
            raise Exception(f"Failed to time travel to snapshot {snapshot_id}: {str(e)}")
        
    def run_vacuum(self, table_name: str) -> None:
        """
        Run a vacuum operation on an Iceberg table.
        
        Args:
            table_name (str): Name of the table to vacuum   
        """
        try:
            # Define the vacuum SQL command
            vacuum_query = f"""
            CALL {config['database']['catalog']}.system.remove_orphan_files(table => '{config['database']['namespace']}.{table_name}', prefix_mismatch_mode => 'IGNORE')
            """

            self.spark.sql(vacuum_query)
            
            print(f"Vacuum job successfully run on table {config['database']['namespace']}.{table_name}")

            self.copy_metrics_to_db()
        except Exception as e:
            print(f"Error running vacuum job: {str(e)}")
        
    def copy_metrics_to_db(self):
       self.repo.copy_metadata_to_db(self.spark, config['database']['catalog'], config['database']['namespace'])
                