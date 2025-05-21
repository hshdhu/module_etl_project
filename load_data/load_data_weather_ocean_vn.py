import load
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_path = os.path.join(parent_dir, "data_input","data_weather")
load.load_data(
        "data_weather_ocean_vn",
        load_path,
    )
