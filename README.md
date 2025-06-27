# Task 2: Data Model and ETL Documentation

This document describes the main tables and ETL logic for the Money In data pipeline.  
It includes table definitions, sample data, and explanations for each component.

---

## 1. Table: `Currencies`

**Purpose:**  
Stores exchange rates between currency pairs for specific dates.  
Used to convert transaction amounts to USD or other currencies.

**Columns:**
- `CurrencyFrom`: The source currency code (e.g., "EUR").
- `CurrencyTo`: The target currency code (e.g., "USD").
- `Rate`: The exchange rate from source to target.
- `Date`: The date the rate is valid for.

```sql
DROP TABLE IF EXISTS Currencies;

CREATE TABLE Currencies (
   CurrencyFrom VARCHAR(3) NOT NULL,
   CurrencyTo   VARCHAR(3) NOT NULL,
   Rate         DECIMAL(18,6) NOT NULL,
   Date         DATE NOT NULL,
   PRIMARY KEY (CurrencyFrom, CurrencyTo, Date)
);

INSERT INTO Currencies (CurrencyFrom, CurrencyTo, Rate, Date) VALUES
('EUR', 'CZK', 26.0000,   '2017-07-21'),
('EUR', 'DKK', 7.4593,    '2017-07-25'),
('EUR', 'GBP', 0.8337,    '2017-07-20'),
('PLN', 'USD', 4.1543,    '2017-07-25'),
('CAD', 'USD', 2.0000,    '2017-07-25'),
('GBP', 'USD', 1.3791,    '2017-07-30'),
('USD', 'EUR', 1.1124,    '2017-07-30'),
('EUR', 'USD', 1.6000,    '2017-07-30');
```

---

## 2. Table: `Account_MoneyIn`

**Purpose:**  
Records incoming money transactions into customer accounts.

**Columns:**
- `TransactionID`: Unique transaction identifier.
- `CustomerID`: Customer receiving the money.
- `TransactionDate`: Date of the transaction.
- `Currency`: Transaction currency.
- `Amount`: Amount received.
- `CompanyID`: Associated company.
- `CompanyCategory`: Company type/category.
- `UpdateDate`: Last update date for the record.

```sql
DROP TABLE IF EXISTS Account_MoneyIn;

CREATE TABLE Account_MoneyIn (
   TransactionID   BIGINT PRIMARY KEY,
   CustomerID      BIGINT NOT NULL,
   TransactionDate DATE NOT NULL,
   Currency        VARCHAR(3) NOT NULL,
   Amount          DECIMAL(18,4) NOT NULL,
   CompanyID       BIGINT NOT NULL,
   CompanyCategory VARCHAR(50) NOT NULL,
   UpdateDate      DATE NOT NULL
);

INSERT INTO Account_MoneyIn (TransactionID, CustomerID, TransactionDate, Currency, Amount, CompanyID, CompanyCategory, UpdateDate) VALUES
(4256780, 103658, '2017-07-21', 'USD', 150.00, 1256, 'Marketplace', '2017-04-03'),
(4256782, 103700, '2017-07-25', 'GBP', 144.06, 20156, 'Freelance', '2017-04-03'),
(4256783, 103700, '2017-07-20', 'GBP', 34.99, 20156, 'Freelance', '2017-04-03'),
(4256785, 103700, '2017-07-25', 'USD', 42.99, 4456, 'Freelance', '2018-04-03'),
(4256786, 103700, '2017-07-25', 'USD', 30.87, 4456, 'Freelance', '2022-04-03'),
(4256787, 103700, '2017-07-30', 'GBP', 30.87, 4456, 'Freelance', '2024-04-03');
```

---

## 3. Table: `Card_MoneyIn`

**Purpose:**  
Logs incoming money transactions made via card payments.

**Columns:**
- `TransactionID`: Unique transaction identifier.
- `CustomerID`: Customer receiving the money.
- `TransactionDate`: Date of the transaction.
- `Currency`: Transaction currency.
- `Amount`: Amount received.
- `CompanyID`: Associated company.
- `CompanyCategory`: Company type/category.
- `UpdateDate`: Last update date for the record.

```sql
DROP TABLE IF EXISTS Card_MoneyIn;

CREATE TABLE Card_MoneyIn (
   TransactionID   BIGINT PRIMARY KEY,
   CustomerID      BIGINT NOT NULL,
   TransactionDate DATE NOT NULL,
   Currency        VARCHAR(3) NOT NULL,
   Amount          DECIMAL(18,4) NOT NULL,
   CompanyID       BIGINT NOT NULL,
   CompanyCategory VARCHAR(50) NOT NULL,
   UpdateDate      DATE NOT NULL
);

INSERT INTO Card_MoneyIn (TransactionID, CustomerID, TransactionDate, Currency, Amount, CompanyID, CompanyCategory, UpdateDate) VALUES
(4256778, 103658, '2017-07-20', 'USD', 200.00, 1256, 'Marketplace', '2024-04-03'),
(4256779, 103658, '2107-07-21', 'EUR', 30.00, 1256, 'Marketplace', '2022-04-03'),
(4256781, 103658, '2017-07-22', 'EUR', 153.00, 20156, 'Freelance', '2021-04-03'),
(4256784, 103700, '2017-07-23', 'CAD', 27.78, 20156, 'Freelance', '2020-04-03');
```

---

## 4. Table: `Fact_Money_In`

**Purpose:**  
A fact table that consolidates all "Money In" transactions (from both account and card sources), enriches them with additional details, and converts all amounts to USD using the latest available exchange rate.

**Columns:**
- `TransactionID`: Unique transaction identifier.
- `SourceType`: Source of the transaction ('Account' or 'Card').
- `CustomerID`: Customer receiving the money.
- `TransactionDate`: Date of the transaction.
- `Currency`: Transaction currency.
- `Amount`: Amount received.
- `Amount_USD`: Amount converted to USD.
- `CompanyID`: Associated company.
- `CompanyCategory`: Company type/category.
- `TransactionMethod`: Method of transaction (from extended details).
- `In_fee`: Incoming fee (from extended details).
- `LastUpdateDate`: Last update date for the record.

```sql
DROP TABLE IF EXISTS Fact_Money_In;

CREATE TABLE Fact_Money_In (
   TransactionID      BIGINT PRIMARY KEY,
   SourceType         VARCHAR(10),      -- 'Account' or 'Card'
   CustomerID         BIGINT,
   TransactionDate    DATE,
   Currency           VARCHAR(3),
   Amount             DECIMAL(18,4),
   Amount_USD         DECIMAL(18,4),
   CompanyID          BIGINT,
   CompanyCategory    VARCHAR(50),
   TransactionMethod  VARCHAR(20),
   In_fee             DECIMAL(18,4),
   LastUpdateDate     DATE
);
```

---

## 5. ETL Logic for `Fact_Money_In`

**Overview:**  
The ETL process for populating `Fact_Money_In` involves:
- Selecting the latest version of each transaction from both `Account_MoneyIn` and `Card_MoneyIn`
- Optionally joining with extended transaction details (if available)
- Converting all amounts to USD using the latest available exchange rate
- Inserting or updating the fact table

**Key SQL Steps:**

```sql
-- Latest Account_MoneyIn
CREATE OR REPLACE VIEW Latest_Account AS
SELECT *
FROM Account_MoneyIn a1
WHERE UpdateDate = (
   SELECT MAX(UpdateDate)
   FROM Account_MoneyIn a2
   WHERE a2.TransactionID = a1.TransactionID
);

-- Latest Card_MoneyIn
CREATE OR REPLACE VIEW Latest_Card AS
SELECT *
FROM Card_MoneyIn c1
WHERE UpdateDate = (
   SELECT MAX(UpdateDate)
   FROM Card_MoneyIn c2
   WHERE c2.TransactionID = c1.TransactionID
);

-- Latest MoneyIn_Extended_Details (if exists)
CREATE OR REPLACE VIEW Latest_Extended AS
SELECT *
FROM MoneyIn_Extended_Details m1
WHERE UpdateDate = (
   SELECT MAX(UpdateDate)
   FROM MoneyIn_Extended_Details m2
   WHERE m2.TransactionID = m1.TransactionID
);

-- Union all money-in transactions
CREATE OR REPLACE VIEW All_MoneyIn AS
SELECT
   TransactionID, 'Account' AS SourceType, CustomerID, TransactionDate, Currency, Amount, CompanyID, CompanyCategory, UpdateDate AS LastUpdateDate
FROM Latest_Account
UNION ALL
SELECT
   TransactionID, 'Card' AS SourceType, CustomerID, TransactionDate, Currency, Amount, CompanyID, CompanyCategory, UpdateDate AS LastUpdateDate
FROM Latest_Card;

-- Join with extended details
CREATE OR REPLACE VIEW MoneyIn_With_Extended AS
SELECT
   m.*,
   e.TransactionMehtod,
   e.In_fee,
   GREATEST(m.LastUpdateDate, IFNULL(e.UpdateDate, m.LastUpdateDate)) AS MaxUpdateDate
FROM All_MoneyIn m
LEFT JOIN Latest_Extended e ON m.TransactionID = e.TransactionID;

-- Add exchange rate to USD
CREATE OR REPLACE VIEW MoneyIn_With_Rate AS
SELECT
   m.*,
   (
       SELECT Rate
       FROM Currencies c
       WHERE c.CurrencyFrom = m.Currency
         AND c.CurrencyTo = 'USD'
         AND c.Date <= m.TransactionDate
       ORDER BY c.Date DESC
       LIMIT 1
   ) AS RateToUSD
FROM MoneyIn_With_Extended m;

-- Prepare staging for fact table
CREATE OR REPLACE VIEW Fact_MoneyIn_Staging AS
SELECT
   TransactionID,
   SourceType,
   CustomerID,
   TransactionDate,
   Currency,
   Amount,
   CASE
       WHEN Currency = 'USD' THEN Amount
       WHEN RateToUSD IS NOT NULL THEN Amount * RateToUSD
       ELSE NULL
   END AS Amount_USD,
   CompanyID,
   CompanyCategory,
   TransactionMehtod AS TransactionMethod,
   In_fee,
   MaxUpdateDate AS LastUpdateDate
FROM MoneyIn_With_Rate;

-- Insert or update fact table
INSERT INTO Fact_Money_In
SELECT * FROM Fact_MoneyIn_Staging
ON DUPLICATE KEY UPDATE
   SourceType = VALUES(SourceType),
   CustomerID = VALUES(CustomerID),
   TransactionDate = VALUES(TransactionDate),
   Currency = VALUES(Currency),
   Amount = VALUES(Amount),
   Amount_USD = VALUES(Amount_USD),
   CompanyID = VALUES(CompanyID),
   CompanyCategory = VALUES(CompanyCategory),
   TransactionMethod = VALUES(TransactionMethod),
   In_fee = VALUES(In_fee),
   LastUpdateDate = VALUES(LastUpdateDate);
```

---

**Note:**  
- The ETL assumes the existence of a table `MoneyIn_Extended_Details` for additional transaction details.
- All views are used to simplify the logic and ensure only the latest version of each transaction is used.
- Amounts are converted to USD using the most recent exchange rate available on or before the transaction date.

--- 
