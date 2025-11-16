import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017")
db = client.bigdata_project
col = db.sentiment

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Real-time Sentiment Dashboard"),

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

    dcc.Interval(id="refresh", interval=5000)
])

@app.callback(
    Output("graph", "figure"),
    Input("sentiment", "value"),
    Input("refresh", "n_intervals")
)
def update_chart(sentiment_type, _):
    docs = list(col.find({"sentiment": sentiment_type}))
    if not docs:
        return px.line()

    df = pd.DataFrame(docs)
    df["time"] = df["window"].apply(lambda w: w["start"])

    fig = px.line(df, x="time", y="count", title=f"{sentiment_type} trend")
    return fig

app.run_server(debug=True)
