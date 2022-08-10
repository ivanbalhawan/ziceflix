from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String




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
                                       f" series_title = \"{series_title}\" AND season_number = {season_number}"))
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
                                       


if __name__ == "__main__":
    db_tool = DBTool(path="media.db")
    db_tool.fetch()

