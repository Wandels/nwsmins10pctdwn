import databento as db
from dotenv import load_dotenv
import os
import pandas as pd
#
load_dotenv(dotenv_path="../.ENV")
api_key = os.environ.get("test")

client = db.Live(key=api_key) #WE DO NOT HAVE ACCESS TO LIVE DATA YET

client.subscribe(
    dataset="GLBX.MDP3",
    schema="ohlcv-1s",
    stype_in="parent",
    symbols=["ES.FUT", "NQ.FUT"],
)

client.add_callback(print)

client.start()

client.block_for_close(timeout=10)
