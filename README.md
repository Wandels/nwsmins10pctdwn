###### How I spent my Junior Fall Semester and Winter Break:  
### Mean-Reversion/Trend-Following Strategy developed in Python with Databento, Polygon, and MongoDB 
#### A big thank you to [Databento](https://databento.com/) for their data sponsorship which made this project possible. I cannot recommend their API enough.
###### Please email andelman.w@wustl.edu with any questions, criticisms, or tips for improvement!
___
### Hypothesis:  
*Stocks that experience significant and sudden drops are likely to continue their decline briefly before reverting to their mean value.*  
In essence, the strategy involves analyzing the top 3000 US Equities by liquidity or market capitalization, identifying those that experienced the *fastest* 10% drop within a rolling 60-day window. For each qualifying stock, I calculate a weight based on the number of days elapsed since the fastest drop, using the arctan function to scale the weight between 0 and $\frac{\pi}{2}$. This scaling emphasizes more recent drops (momentum), allowing for a dynamic adjustment between short and long positions over time as signals age.  

To ensure market neutrality and to create the initial short positions, I adjust these initial weights by subtracting their mean, effectively centering the distribution around zero. This adjustment serves two purposes:
1. it ensures that for every short position, there's a corresponding long position to hedge it, and it also fine-tunes the strategy's responsiveness to recent vs. older price drops.
2. Stocks with more recent drops (and thus weights less than the mean, closer to 0) become negative, representing larger short positions, while those representing older drops are kept long.

Finally, to comply with the CQA challenge guidelines, I cap the weights at 4.9% & normalize to ensure the portfolio's total weight sums to 1.  
This step ensures the strategy remains practical and within risk management parameters.
___
### Conclusion
If you are here for an edge in the market, I am sorry to disappoint. Over the short backtesting window I could afford, this strategy **did not show promising profitability**. There is either a flaw in my hypothesis (Bad Alpha), a mistake in my strategy logic code, or most likely, a mistake in my backtest. There were multiple points in the development process where I faced difficult decisions and was forced to make assumptions. I have come to learn that this is often what Algorithmic Development is. Not everything can be backtested and if you ever want your algorithm to see the light of day, it takes a leap of faith.
Nonetheless, the educational experience I gained from this was priceless. 
