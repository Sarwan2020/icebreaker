from typing import List, Optional
import duckdb
from pyspark.sql.functions import lit

class DuckDBRepository:
    def __init__(self, db_path:str,catalog:str,schema:str):
        self.conn = duckdb.connect(db_path)
        self.catalog = catalog
        self.schema = schema

    def get_all_tables(self):
        query = f'''select 
                    distinct source_table
                    from {self.catalog}.{self.schema}.all_data_files'''
        result = self.conn.sql(query)
        return result
    
    def get_active_files(self,):
        query = f'''select 
                *
                from {self.catalog}.{self.schema}.files
                where  content = 0;'''
        result = self.conn.sql(query)
        return result

    def get_all_files(self):
        query = f'''select distinct file_path,
                    file_format,
                    spec_id,
                    partition,
                    record_count,
                    file_size_in_bytes,
                    column_sizes,
                    value_counts,
                    source_table
                    from {self.catalog}.{self.schema}.all_data_files
                    '''
        result = self.conn.sql(query)
        return result

    def get_history(self):
        query = f'''select 
                    *
                    from {self.catalog}.{self.schema}.history'''
        result = self.conn.sql(query)
        return result
    
    def get_snapshot_details(self):
        query = f'''select 
                    *
                    from {self.catalog}.{self.schema}.snapshots'''
        result = self.conn.sql(query)
        return result
    
    def all_table_growth_metrics(self):
        query = f"""
        WITH ranked_snapshots AS (
            SELECT 
                source_table,
                CAST(committed_at AS DATE) AS date,
                CAST(summary['total-records'] AS BIGINT) AS total_records,
                CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
                CAST(summary['total-files-size'] AS DOUBLE) / 1024 / 1024 AS total_file_size,
                ROW_NUMBER() OVER (PARTITION BY source_table,CAST(committed_at AS DATE) ORDER BY committed_at DESC) as row_num
            FROM {self.catalog}.{self.schema}.snapshots
        )
        SELECT 
        source_table,   
            date,
            total_records,
            total_data_files,
            total_file_size
        FROM ranked_snapshots
        WHERE row_num = 1
        order by source_table,date desc
        """
        result = self.conn.sql(query)
        return result
    
    def get_partition_details(self):
        query = f'''select 
                    *
                    from {self.catalog}.{self.schema}.partitions'''
        result = self.conn.sql(query)
        return result       
    
    def metadata_log_entries(self):
        query = f'''select * from {self.catalog}.{self.schema}.metadata_log_entries
          order by timestamp desc'''
        result = self.conn.sql(query)
        return result
    
    def copy_metadata_to_db(self, spark, catalog, namespace):
         # create a connection to a file called 'file.db'


    # Get list of tables
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}")
        # Debug: Print column names
        print("Available columns:", tables_df.columns)
        tables_to_process = [row.tableName for row in tables_df.collect()]
        #tables_to_process = ['inventory_info']

        print(f"Found {len(tables_to_process)} tables to process: {tables_to_process}")

        # Create the ocsf schema once at the beginning
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS ocsf;")

        for table_name in tables_to_process:
            print(f"\n=== Processing table: {table_name} ===")
            
            # Create a temporary directory for parquet files
            import tempfile
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__))
            temp_dir = os.path.join(script_dir, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            print(f"Created temporary directory: {temp_dir}")

            # Modified function to include table_name in the data
            def process_table(query, table_suffix, temp_dir, table_name):
                print(f"\nProcessing metadata table: {table_suffix}")
                parquet_dir = os.path.join(temp_dir, f"{table_name}_{table_suffix}")
                print(f"Executing query: {query}")
                try:
                    df = spark.sql(query)
                    if df.isEmpty():
                        print(f"WARNING: Query returned no data for {table_suffix}")
                        return
                    
                    # Add table_name column to the DataFrame
                    df = df.withColumn("source_table", lit(table_name))

                    print(f"Writing to parquet directory: {parquet_dir}")
                    df.write.mode("overwrite").parquet(parquet_dir)
                    
                    if not os.path.exists(parquet_dir):
                        print(f"ERROR: Parquet directory was not created at {parquet_dir}")
                        return
                    
                    print(f"Loading into DuckDB: ocsf.{table_suffix}")
                    # Modified to create or append to the table
                    if table_name == tables_to_process[0]:  # First table
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE ocsf.{table_suffix} AS 
                            SELECT * FROM read_parquet('{parquet_dir}/*parquet')
                        """)
                    else:  # Subsequent tables
                        self.conn.execute(f"""
                            INSERT INTO ocsf.{table_suffix}
                            SELECT * FROM read_parquet('{parquet_dir}/*parquet')
                        """)
                    print(f"Successfully processed {table_suffix}")
                except Exception as e:
                    print(f"ERROR processing {table_suffix}: {str(e)}")
                    print(f"Query that failed: {query}")
                    raise

            # Process metadata tables
            metadata_tables = [
                ("history", f"SELECT * FROM {catalog}.{namespace}.{table_name}.history"),
                ("metadata_log_entries", f"SELECT * FROM {catalog}.{namespace}.{table_name}.metadata_log_entries"),
                ("snapshots", f"SELECT * FROM {catalog}.{namespace}.{table_name}.snapshots"),
                ("files", f"SELECT * FROM {catalog}.{namespace}.{table_name}.files"),
                ("manifests", f"SELECT * FROM {catalog}.{namespace}.{table_name}.manifests"),
                ("partitions", f"SELECT * FROM {catalog}.{namespace}.{table_name}.partitions"),
                ("all_data_files", f"SELECT * FROM {catalog}.{namespace}.{table_name}.all_data_files"),
                ("all_manifests", f"SELECT * FROM {catalog}.{namespace}.{table_name}.all_manifests"),
                ("refs", f"SELECT * FROM {catalog}.{namespace}.{table_name}.refs"),
                ("entries", f"SELECT * FROM {catalog}.{namespace}.{table_name}.entries")
            ]

            for table_suffix, query in metadata_tables:
                try:
                    process_table(query, table_suffix, temp_dir, table_name)
                except Exception as e:
                    print(f"ERROR processing {table_suffix}: {str(e)}")
                    print(f"Query that failed: {query}")

            print(f"\nCleaning up temporary directory: {temp_dir}")
            import shutil
            #shutil.rmtree(temp_dir)
            print(f"Finished processing {table_name}\n")
    
    def close(self):
        self.conn.close()