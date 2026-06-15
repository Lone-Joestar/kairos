import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from factory.dag_builder import build_dag, load_sources

sources = load_sources()

# Explicitly create module-level variables for each DAG
# Airflow's DagBag scanner looks for DAG objects at module level
dag_science_and_technology = build_dag(sources["categories"][0])
dag_education = build_dag(sources["categories"][1])
dag_entertainment = build_dag(sources["categories"][2])
dag_news_and_politics = build_dag(sources["categories"][3])
dag_music = build_dag(sources["categories"][4])