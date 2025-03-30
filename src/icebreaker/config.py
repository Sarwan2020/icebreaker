from pathlib import Path
import yaml
import os

def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file"""
    if config_path is None:
        config_path =  "icebreaker/config.yaml"
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_config_path():
    # Get the current file's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Go up two levels to reach the project root
    project_root = os.path.dirname(os.path.dirname(current_dir))
    
    # Construct path to config.yaml
    config_path = os.path.join(project_root, "config.yaml")
    
    return config_path



# Use this in your code
config_path = get_config_path()

# Load config once at module level
config = load_config(config_path)