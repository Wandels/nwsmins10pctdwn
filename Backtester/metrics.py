class Metrics:
    def __init__(self, booksize):
        self.booksize = booksize

    async def total_shares(self, portfolio):
        '''Compute total shares'''
        total_shares = sum(abs(item['shares']) for item in portfolio.values())
        return total_shares

    async def total_market_value(self, portfolio):
        '''Compute total market value'''
        total_market_value = sum(abs(item['value']) for item in portfolio.values())
        return total_market_value

#To Do
    async def pnl(self, portfolio, status):
        '''calculate pnl'''


    async def sharpe(self):
        '''Find Sharpe'''
    async def turnover(self):
        '''Turnover'''
    async def drawdown(self):
        '''maximum drawdown'''
