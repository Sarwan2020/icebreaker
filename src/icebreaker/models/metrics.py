from repository.duckdb_repo import DuckDBRepository
from config import config

class MetricsService:
    def __init__(self):
        # Initialize repository if not provided
        self.repository = DuckDBRepository(config["database"]["duckdb_path"])