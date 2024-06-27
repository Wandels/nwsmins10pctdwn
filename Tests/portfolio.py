class Portfolio_Tests:
    def __init__(self, booksize):
        self.booksize = booksize

    async def check_total_market_value(self, portfolio):
        '''Compute and Check total market value'''
        total_market_value = sum(abs(item['value']) for item in portfolio.values())
        if (total_market_value > self.booksize):
            print(f"Portfolio exceeded cash balance by {total_market_value - self.booksize}")
        else:
            print(f"Portfolio is within cash balance by {self.booksize - total_market_value}")
        return total_market_value

    async def check_truncation(self, portfolio):
        '''Check if any position exceeds 9.9% of the portfolio'''
        total_market_value = sum(abs(item['value']) for item in portfolio.values())
        max_allowed_value = 0.099 * total_market_value  # 9.9% of total market value

        for ticker, position in portfolio.items():
            if abs(position['value']) > max_allowed_value:
                print(f"Truncation error for {ticker}: position exceeds 9.9% of portfolio - Value: {position['value']}")
                return False
        print("No truncation issues found - all positions are within 9.9% of total portfolio value")
        return True

    async def check_dollar_neutrality(self, portfolio):
        '''Check Dollar Neutrality'''
        longs = sum(position['value'] for position in portfolio.values() if position['value'] > 0)
        shorts = sum(position['value'] for position in portfolio.values() if position['value'] < 0)
        neutrality = longs + shorts
        if neutrality != 0:
            print(f"Dollar neutrality error: difference of {neutrality}")
            return False
        print("Portfolio is dollar neutral")
        return True

    async def count_longs(self, portfolio):
        '''Count Long Positions'''
        long_count = sum(1 for position in portfolio.values() if position['value'] > 0)
        #print(f"Long positions count: {long_count}")
        return long_count

    async def count_shorts(self, portfolio):
        '''Count Short Positions'''
        short_count = sum(1 for position in portfolio.values() if position['value'] < 0)
        #print(f"Short positions count: {short_count}")
        return short_count
