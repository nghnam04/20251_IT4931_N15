import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017")
db = client.bigdata_project
col = db.tweets

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Real-time Sentiment Trend"),
    dcc.Dropdown(
        id="sentiment",
        options=[
            {"label": "Positive", "value": "positive"},
            {"label": "Neutral", "value": "neutral"},
            {"label": "Negative", "value": "negative"}
        ],
        value="positive"
    ),
    dcc.Graph(id="graph"),
    dcc.Interval(
        id="refresh",
        interval=5000
    )
])

@app.callback(
    Output("graph", "figure"),
    Input("sentiment", "value"),
    Input("refresh", "n_intervals")
)
def update(selected, _):
    docs = list(col.find({"sentiment": selected}))
    if not docs:
        return px.line()
    df = pd.DataFrame(docs)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    fig = px.line(df, x="timestamp", y="count")
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
