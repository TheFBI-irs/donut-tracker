import statistics


def compute_returns(prices):
    returns = []

    for i in range(1, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]

        if prev == 0:
            continue

        returns.append((curr - prev) / prev)

    return returns


def rolling_volatility(prices, window=20):
    if len(prices) < window + 1:
        return None

    returns = compute_returns(prices[-window:])
    if len(returns) < 2:
        return None

    return statistics.stdev(returns)


def crash_risk(prices):
    if len(prices) < 40:
        return False

    recent_vol = rolling_volatility(prices, 20)
    historical_vol = rolling_volatility(prices, len(prices) - 1)

    if not recent_vol or not historical_vol:
        return False

    trend = prices[-1] - prices[-5]

    return recent_vol > historical_vol * 1.8 and trend < 0