DROP TABLE IF EXISTS retail_store_sales_cleaned;
CREATE TABLE retail_store_sales_cleaned AS
SELECT * FROM retail_store_sales WHERE 0;

-- Step 2: Median Price Per Unit by Item
DROP TEMPORARY TABLE IF EXISTS MedianPrice;
CREATE TEMPORARY TABLE MedianPrice AS
SELECT Item, `Price Per Unit` AS MedianPrice
FROM (
  SELECT
    Item,
    `Price Per Unit`,
    ROW_NUMBER() OVER (PARTITION BY Item ORDER BY `Price Per Unit`) AS rn,
    COUNT(*) OVER (PARTITION BY Item) AS cnt
  FROM retail_store_sales
  WHERE `Price Per Unit` IS NOT NULL
) ranked
WHERE rn = FLOOR((cnt + 1) / 2);

-- Step 3: Median Price Per Unit by Category
DROP TEMPORARY TABLE IF EXISTS MedianPriceCat;
CREATE TEMPORARY TABLE MedianPriceCat AS
SELECT Category, `Price Per Unit` AS MedianPriceCat
FROM (
  SELECT
    Category,
    `Price Per Unit`,
    ROW_NUMBER() OVER (PARTITION BY Category ORDER BY `Price Per Unit`) AS rn,
    COUNT(*) OVER (PARTITION BY Category) AS cnt
  FROM retail_store_sales
  WHERE `Price Per Unit` IS NOT NULL
) ranked
WHERE rn = FLOOR((cnt + 1) / 2);

-- Step 4: Median Quantity by Item
DROP TEMPORARY TABLE IF EXISTS MedianQty;
CREATE TEMPORARY TABLE MedianQty AS
SELECT Item, Quantity AS MedianQty
FROM (
  SELECT
    Item,
    Quantity,
    ROW_NUMBER() OVER (PARTITION BY Item ORDER BY Quantity) AS rn,
    COUNT(*) OVER (PARTITION BY Item) AS cnt
  FROM retail_store_sales
  WHERE Quantity IS NOT NULL
) ranked
WHERE rn = FLOOR((cnt + 1) / 2);

-- Step 5: Most common item per category
DROP TEMPORARY TABLE IF EXISTS MostCommonItem;
CREATE TEMPORARY TABLE MostCommonItem AS
SELECT Category, Item AS CommonItem
FROM (
  SELECT Category, Item, COUNT(*) AS freq,
         ROW_NUMBER() OVER (PARTITION BY Category ORDER BY COUNT(*) DESC) AS rnk
  FROM retail_store_sales
  WHERE Item IS NOT NULL
  GROUP BY Category, Item
) ranked
WHERE rnk = 1;

-- Step 6: Populate the cleaned table with imputed values
INSERT INTO retail_store_sales_cleaned (
  `Transaction ID`, `Customer ID`, `Category`, `Item`, `Price Per Unit`,
  `Quantity`, `Total Spent`, `Payment Method`, `Location`, `Transaction Date`, `Discount Applied`
)
SELECT
  s.`Transaction ID`,
  s.`Customer ID`,
  s.`Category`,
  COALESCE(s.Item, mci.CommonItem, CONCAT('Item_Unknown_', s.Category)) AS Item,
  COALESCE(s.`Price Per Unit`, mp.MedianPrice, mpc.MedianPriceCat) AS `Price Per Unit`,
  COALESCE(s.Quantity, mq.MedianQty) AS Quantity,
  COALESCE(s.`Total Spent`,
           COALESCE(s.`Price Per Unit`, mp.MedianPrice, mpc.MedianPriceCat) *
           COALESCE(s.Quantity, mq.MedianQty)) AS `Total Spent`,
  s.`Payment Method`,
  s.Location,
  s.`Transaction Date`,
  
  -- This ensures NO NULLS or BLANKS
  CASE
    WHEN s.`Discount Applied` IS NOT NULL AND s.`Discount Applied` != '' THEN s.`Discount Applied`
    WHEN (s.`Total Spent` IS NULL OR s.`Price Per Unit` IS NULL OR s.Quantity IS NULL) THEN 'Unknown'
    WHEN s.`Total Spent` < s.`Price Per Unit` * s.Quantity THEN 'True'
    ELSE 'False'
  END AS `Discount Applied`

FROM retail_store_sales s
LEFT JOIN MedianPrice mp ON s.Item = mp.Item
LEFT JOIN MedianPriceCat mpc ON s.Category = mpc.Category
LEFT JOIN MedianQty mq ON s.Item = mq.Item
LEFT JOIN MostCommonItem mci ON s.Category = mci.Category;

SELECT * FROM retail_store_sales_cleaned;