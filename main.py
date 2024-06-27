from StrategyLogic.historical_data import *
from DataManager.mongo import *
from dotenv import load_dotenv
from Backtester.dates import Backtest, Strategy
import os

load_dotenv(dotenv_path=".ENV")
api_key = os.environ.get("test")
mongo_username = os.environ.get("MONGO_USER")
mongo_pass = os.environ.get("MONGO_PASS")

async def run_strategy_for_period(start_date, end_date, dates_df, stock_list, method):

    progress_tracker_tracker = ProgressTracker(len(stock_list))
    strategy_instance = StrategyLogic(stock_list, start_date, end_date, dates_df)
    mongo_instance = Mongo.get_instance('newsmins10pctdwn')

    market_neutral, arc_tan, days_ago  = await strategy_instance.start_strategy(stock_list, start_date, end_date, dates_df, mongo_instance, method, extended_time=False)


    # Calculate the absolute sum of weights for normalization
    abs_total_weight = sum(abs(weight) for weight in market_neutral.values())

    # Truncating weights based on absolute values
    truncated_weights = {ticker: (weight if abs(weight) <= 0.099 else 0.099 * np.sign(weight)) for ticker, weight in
                         market_neutral.items()}

    abs_total_weight_truncated = sum(abs(weight) for weight in truncated_weights.values())

    # Normalizing truncated weights
    normalized_weights = {ticker: weight / abs_total_weight_truncated for ticker, weight in truncated_weights.items()}

    abs_total_weight_normalized = sum(abs(weight) for weight in normalized_weights.values())

    # Display the market_neutrals
    #print(f"Absolute total weight: {abs_total_weight}")
    # print(f"Truncated weights: {truncated_weights}")
    # print(f"Normalized weights: {normalized_weights}")

    # print(f"Absolute total weight truncated: {abs_total_weight_truncated}")
    # print(f"Absolute total weight norm: {abs_total_weight_normalized}")


    return normalized_weights, market_neutral, arc_tan, days_ago


if __name__ == "__main__":

    earliest_date = '2018-05-01'  # BENTO NASDAQ ITCH
    backtest = Backtest('NASDAQ')
    strategy = Strategy(backtest)
    start_date = strategy.get_n_trading_day_ago(0, full=True)
    print(start_date)
    dates_df = backtest.get_trading_days(earliest_date, start_date)
    end_date = (dates_df.iloc[59].name.strftime('%Y-%m-%d'))
    # print(f"start_date: {start_date}, end_date: {end_date}, dates_df: {dates_df}")
    print(f"For this to work, start date must be {start_date}, end_date must be {end_date} and dates_df must be {dates_df}")
    asyncio.run(run_strategy_for_period(start_date, end_date, dates_df, 'market_cap'))

