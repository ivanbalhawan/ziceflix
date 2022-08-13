from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String
from pathlib import Path
import pandas as pd




class DBTool():
    def __init__(self,
                 path):
        self.engine = create_engine(f"sqlite+pysqlite:///{path}", echo=True, future=True)
        self.metadata_obj = MetaData()

        self.series_table = Table(
            "series",
            self.metadata_obj,
            Column("id", Integer, primary_key=True),
            Column("series_title", String),
            Column("season_number", Integer),
            Column("episode_number", Integer),
            Column("episode_title", String),
            Column("episode_path", String)
        )
        self.metadata_obj.create_all(self.engine)


    def insert(self, data):
        """
        inputs
        ------
        data: List[Dict{}]
            "series_title"
            "season_number"
            "episode_number"
            "episode_title"
            "episode_path"
        returns: None
        """

        with self.engine.connect() as conn:
            conn.execute(
                text("INSERT INTO series (series_title, season_number, episode_number, episode_title, episode_path)"
                     "VALUES(:series_title, :season_number, :episode_number, :episode_title, :episode_path)"),
                data
            )
            conn.commit()
    
    def fetch_series_list(self):
        data = []
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT series_title FROM series"))
            for row in result:
                data.append(row.series_title)
            return data

    def fetch_seasons(self, series_title):
        data = []
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT DISTINCT season_number FROM series "
                                       f"WHERE series_title = \"{series_title}\""))
            for row in result:
                data.append(row.season_number)
            return data

    def fetch_episode_list(self, series_title, season_number):
        data = []
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM series WHERE "
                                       f" series_title = \"{series_title}\" AND season_number = {season_number}"
                                       " ORDER BY episode_number ASC"))
            for row in result:
                data.append({"episode_number": row.episode_number,
                             "episode_title": row.episode_title,
                             "episode_path": row.episode_path})
            return data

    def fetch_episode_path(self, series_title, season_number, episode_number):
        data = []
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT episode_path FROM series WHERE "
                                       f" series_title = \"{series_title}\" AND season_number = {season_number} "
                                       f" AND episode_number = {episode_number}"))
            for row in result:
                data.append({"path": row.episode_path})
            return data

    def fetch_all_series(self):
        # data = []
        query = text("SELECT * FROM series")
        with self.engine.connect() as conn:
            all_series_df = pd.read_sql(sql=query,
                                        con=conn)
        return all_series_df

        # with self.engine.connect() as conn:
        #     result = conn.execute(text("SELECT * FROM series"))
        #     for row in result:
        #         # append row as dict
        #         # or just get the full result as df
        #         pass

    def lookup_series(self):
        series_data = []
        media_dir = Path("static/media/series")
        for series_dir in media_dir.iterdir():
            for season_dir in series_dir.iterdir():
                for episode_path in season_dir.iterdir():
                    series_title = series_dir.name
                    season_number = int(season_dir.name.strip())
                    episode_number = int(episode_path.stem.split('-')[0].strip())
                    episode_title = episode_path.stem.split('-')[1].strip()
                    series_data.append({
                        'series_title': series_title,
                        'season_number': season_number,
                        'episode_number': episode_number,
                        'episode_title': episode_title,
                        'episode_path': str(episode_path).split('/', 1)[1]
                    })
        series_df = pd.DataFrame(series_data)
        return series_df

    def update_db(self):
        db_series = self.fetch_all_series()
        all_series = self.lookup_series()
        all_series = pd.concat([all_series, db_series])
        all_series = all_series.drop_duplicates(subset=["series_title",
                                                        "season_number",
                                                        "episode_number"], keep=False)
        insert_data = all_series[['series_title',
                                  'season_number',
                                  'episode_number',
                                  'episode_title',
                                  'episode_path']].to_dict(orient='records')
        self.insert(insert_data)

                    



if __name__ == "__main__":
    db_tool = DBTool(path="media.db")
    db_tool.update_db()

