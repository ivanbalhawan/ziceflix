from flask import Flask
from flask import url_for
from flask import render_template
from flask import send_from_directory

from pathlib import Path

from db import DBTool


app = Flask(__name__)

tool = DBTool('media.db')


@app.route("/")
def index():
    series_titles = tool.fetch_series_list()
    return render_template("index.html", series_titles=series_titles)

@app.route("/series/")
def series_page():
    series_titles = tool.fetch_series_list()
    return render_template("series_page.html", series_titles=series_titles)

@app.route("/series/<series_title>")
def series_title_page(series_title):
    seasons = tool.fetch_seasons(series_title)
    return render_template("series_title_page.html", series_title=series_title, seasons=seasons)

@app.route("/series/<series_title>/<season>")
def season_page(series_title, season):
    episodes = tool.fetch_episode_list(series_title, season)
    return render_template("season_page.html", series_title=series_title, season=season, episodes=episodes)

@app.route("/series/<series_title>/<season>/<episode_number>")
def player(series_title, season, episode_number):
    path = tool.fetch_episode_path(series_title=series_title,
                                   season_number=season,
                                   episode_number=episode_number)
    path = path[0]['path']
    return render_template("player.html", path=path)

