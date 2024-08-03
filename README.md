# Algorithmic Trading Strategy with Databento Price Data 
Developed in Python with Databento, Polygon, and MongoDB.

### Acknowledgements
A big thank you to [Databento](https://databento.com/) for their data sponsorship which made this project possible. I highly recommend their API.

For any questions, criticisms, or tips for improvement, please email me at andelman.w@wustl.edu.

---

## Hypothesis
**Stocks that experience significant and sudden drops are likely to continue their decline briefly before reverting to their mean value.**

### Strategy Overview
The strategy involves analyzing the top 3000 US Equities by liquidity or market capitalization, identifying those that experienced the fastest 10% drop within a rolling 60-day window. 

#### Weight Calculation
For each qualifying stock, a weight is calculated based on the number of days elapsed since the fastest drop, using the arctan function to scale the weight between 0 and $\frac{\pi}{2}$. This scaling emphasizes more recent drops (momentum), allowing for a dynamic adjustment between short and long positions over time as signals age.

#### Market Neutrality
To ensure market neutrality and create initial short positions:
1. Initial weights are adjusted by subtracting their mean, centering the distribution around zero.
2. Stocks with more recent drops (weights less than the mean) become negative, representing larger short positions, while those representing older drops are kept long.

#### Risk Management
Weights are capped at 4.9% and normalized to ensure the portfolio's total weight sums to 1, complying with the CQA challenge guidelines.

---

## Conclusion
If you're looking for a market edge, I must disappoint. Over the short backtesting window, this strategy **did not show promising profitability**. The reasons could be:
- A flaw in the hypothesis
- A mistake in the strategy logic code
- A mistake in the backtest

Algorithmic development often involves difficult decisions and assumptions. Despite the lack of profitability, the educational experience gained was invaluable.
