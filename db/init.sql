-- Tables
CREATE TABLE shares (
    id SERIAL PRIMARY KEY,
    address BYTEA NOT NULL UNIQUE,
    twitter_username TEXT,
    twitter_name TEXT,
    twitter_score NUMERIC,
    registered BIGINT NOT NULL,
    last_transaction BIGINT NOT NULL,
    balance NUMERIC NOT NULL,
    buy_price NUMERIC NOT NULL DEFAULT 0,
    sell_price NUMERIC NOT NULL DEFAULT 0,
    supply INTEGER NOT NULL DEFAULT 1,
    rank BIGINT
);

CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    trader BYTEA NOT NULL,
    subject BYTEA NOT NULL,
    is_buy BOOLEAN NOT NULL,
    share_amount INTEGER NOT NULL,
    eth_amount NUMERIC NOT NULL,
    protocol_eth_amount NUMERIC NOT NULL,
    subject_eth_amount NUMERIC NOT NULL,
    supply INTEGER NOT NULL,
    transaction_hash BYTEA NOT NULL UNIQUE,
    block_number BIGINT NOT NULL,
    timestamp BIGINT
);


-- Materialized Views Creation for Efficient Statistics Fetching

-- Net Shares Owned Calculation
CREATE MATERIALIZED VIEW net_shares AS
SELECT
    trader,
    subject,
    SUM(
        CASE 
            WHEN is_buy THEN share_amount
            ELSE -share_amount
        END
    ) AS net_shares
FROM 
    trades
GROUP BY 
    trader, 
    subject;

-- Porfolio Value Calculation
CREATE MATERIALIZED VIEW portfolio_value_per_share AS
SELECT
    ownership.trader AS address,
    SUM(ownership.net_shares * s.buy_price) AS portfolio_value
FROM
    net_shares AS ownership
JOIN
    shares s ON ownership.subject = s.address
GROUP BY
    ownership.trader;

-- Fees Earned Calculation
CREATE MATERIALIZED VIEW fees_earned_per_share AS
SELECT
    s.address,
    COALESCE(SUM(t.subject_eth_amount), 0) AS fees_earned
FROM
    shares s
LEFT JOIN
    trades t ON s.address = t.subject AND t.trader <> t.subject
GROUP BY
    s.address;

-- Holders & Holdings Calculation
CREATE MATERIALIZED VIEW holders_and_holdings_per_share AS
SELECT
    COALESCE(holders.address, holdings.address) AS address,
    COALESCE(holder_count, 0) AS holders,
    COALESCE(holding_count, 0) AS holdings
FROM
    (
        SELECT
            subject AS address,
            COUNT(DISTINCT trader) AS holder_count
        FROM
            net_shares
        WHERE
            net_shares > 0
        GROUP BY
            subject
    ) AS holders
FULL JOIN
    (
        SELECT
            trader AS address,
            COUNT(DISTINCT subject) AS holding_count
        FROM
            net_shares
        WHERE
            net_shares > 0
        GROUP BY
            trader
    ) AS holdings ON holders.address = holdings.address;

-- Values Bought/Sold  & Active Days Calculations
CREATE MATERIALIZED VIEW trading_activity_per_share AS
SELECT
    trades_summary.address,
    COALESCE(sum_bought, 0) AS total_value_bought,
    COALESCE(sum_sold, 0) AS total_value_sold,
    trades_summary.number_of_active_days
FROM
    (
        SELECT 
            trader AS address,
            SUM(
                CASE 
                    WHEN is_buy THEN 
                        CASE
                            WHEN trader = subject THEN (eth_amount + protocol_eth_amount)
                            ELSE (eth_amount + protocol_eth_amount + subject_eth_amount)
                        END
                    ELSE 0
                END
            ) AS sum_bought,
            SUM(
                CASE 
                    WHEN is_buy THEN 0
                    ELSE 
                        CASE
                            WHEN trader = subject THEN (eth_amount + subject_eth_amount)
                            ELSE eth_amount
                        END
                END
            ) AS sum_sold,
            COUNT(DISTINCT DATE_TRUNC('day', TO_TIMESTAMP(timestamp) AT TIME ZONE 'UTC')) AS number_of_active_days
        FROM 
            trades
        GROUP BY 
            trader
    ) AS trades_summary;


CREATE MATERIALIZED VIEW shares_data AS
SELECT
    shares.address,
    shares.twitter_username,
    shares.twitter_name,
    shares.twitter_score,
    shares.registered,
    shares.last_transaction,
    shares.balance,
    shares.buy_price,
    shares.sell_price,
    shares.supply,
    COALESCE(pvps.portfolio_value, 0) AS portfolio_value,
    COALESCE(feps.fees_earned, 0) AS fees_earned,
    COALESCE(hahps.holders, 0) AS holders,
    COALESCE(hahps.holdings, 0) AS holdings,
    COALESCE(taps.total_value_bought, 0) AS total_value_bought,
    COALESCE(taps.total_value_sold, 0) AS total_value_sold,
    COALESCE(taps.number_of_active_days, 0) AS number_of_active_days
FROM
    shares
    LEFT JOIN portfolio_value_per_share pvps ON shares.address = pvps.address
    LEFT JOIN fees_earned_per_share feps ON shares.address = feps.address
    LEFT JOIN holders_and_holdings_per_share hahps ON shares.address = hahps.address
    LEFT JOIN trading_activity_per_share taps ON shares.address = taps.address;


-- Indexes
CREATE INDEX idx_shares_address ON shares(address);
CREATE INDEX idx_shares_twitter_username ON shares(twitter_username);
CREATE INDEX idx_shares_balance ON shares(balance);

CREATE INDEX idx_trades_trader ON trades(trader);
CREATE INDEX idx_trades_subject ON trades(subject);
CREATE INDEX idx_trades_block_number ON trades(block_number);
CREATE INDEX idx_trades_timestamp ON trades(timestamp);

CREATE INDEX idx_net_shares_trader ON net_shares(trader);
CREATE INDEX idx_net_shares_subject ON net_shares(subject);
CREATE INDEX idx_portfolio_value_per_share_address ON portfolio_value_per_share(address);
CREATE INDEX idx_fees_earned_per_share_address ON fees_earned_per_share(address);
CREATE INDEX idx_holders_and_holdings_per_share_address ON holders_and_holdings_per_share(address);
CREATE INDEX idx_trading_activity_per_share_address ON trading_activity_per_share(address);
CREATE INDEX idx_shares_data_address ON shares_data(address);

-- REFRESH MATERIALIZED VIEW net_shares;
-- REFRESH MATERIALIZED VIEW portfolio_value_per_share;
-- REFRESH MATERIALIZED VIEW fees_earned_per_share;
-- REFRESH MATERIALIZED VIEW holders_and_holdings_per_share;
-- REFRESH MATERIALIZED VIEW trading_activity_per_share;
-- REFRESH MATERIALIZED VIEW shares_data;


-- SELECT * FROM shares_data;