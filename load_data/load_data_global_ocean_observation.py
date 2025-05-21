import load
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_path = os.path.join(parent_dir, "output","output_clean_data_global_ocean_observation")
load.load_data(
        "data_global_ocean_observation",
        load_path,
    )
