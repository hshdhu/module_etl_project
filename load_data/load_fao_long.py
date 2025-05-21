import load
import os 
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_path = os.path.join(parent_dir, "output","fao_long_cleaned")
load.load_data(
        "data_fao_long",
        load_path,
    )