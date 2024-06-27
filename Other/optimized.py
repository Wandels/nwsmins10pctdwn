def fetch_single_day_data(raw_symbol, date):
    start_time = f"{date}08:00:00+00:00"
    end_time = f"{date}23:59:00+00:00"
    data = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        schema="ohlcv-1m",
        stype_in="raw_symbol",
        symbols=[raw_symbol],
        start=start_time,
        end=end_time,
    )
    return data.to_df()


def find_time_for_10pct_drop_v2(minute_df):
    start_price = minute_df['high'].iloc[0]
    pct_drops = (start_price - minute_df['low']) / start_price * 100
    idx = pct_drops[pct_drops >= 10].index.min()
    if pd.notnull(idx):
        time_to_drop = idx - minute_df.index[0]
        return idx, time_to_drop
    return None, None


def fetch_and_analyze_data(raw_symbol, extended_time=True):
    drops_10_pct_or_more = fetch_days_10pct_drop(raw_symbol)

    for index, _ in drops_10_pct_or_more.iterrows():
        day_str = index.strftime('%Y-%m-%dT')
        day_minute_df = fetch_single_day_data(raw_symbol, day_str[:10])

        steepest_drop_start, time_to_steepest_drop = find_time_for_10pct_drop_v2(day_minute_df)
        if steepest_drop_start is not None:
            print(
                f"On {day_str}, {raw_symbol} had its steepest 10% drop starting at {steepest_drop_start}, taking {time_to_steepest_drop} minutes.")
            plot_price_data(raw_symbol, day_minute_df, day_str, steepest_drop_start, steepest_drop_start+time_to_steepest_drop)
        else:
            print(f"No steep 10% drop on {day_str}")


fetch_and_analyze_data("AAPL", True)
