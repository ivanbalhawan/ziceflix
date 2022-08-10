from pathlib import Path
from db import DBTool
import re

tool = DBTool("media.db")
path = Path("static/media/series/IASIP/season_1/")
data = []

title_pattern = r'\-\s(\w+)\.avi'
ep_number_pattern = r'Episode\s(\d+)'

series_title = "It's Always Sunny in Philadelphia"
season_number = 1
for i in path.iterdir():
    episode_path = str(i).split('/', 0)[1]
    episode_title = re.findall(title_pattern, str(i))[0]
    episode_number = int(re.findall(ep_number_pattern, str(i)))
    data.append({
        "series_title": series_title,
        "season_number": season_number,
        "episode_number": episode_number,
        "episode_title": episode_title,
        "episode_path": episode_path
    })

tool.insert(data)
