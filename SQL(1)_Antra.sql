-- 1. List of Persons’ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any). 
SELECT TOP (1000) People.FullName, People.PhoneNumber, People.FaxNumber, Customers.CustomerName, Customers.PhoneNumber, Customers.FaxNumber
FROM Application.People
JOIN Sales.Customers
ON SUBSTRING(People.EmailAddress,CHARINDEX('@',People.EmailAddress)+1, CHARINDEX('.',People.EmailAddress,CHARINDEX('$',People.EmailAddress)+1) -CHARINDEX('@',People.EmailAddress)-1)
 = SUBSTRING(
        Customers.WebsiteURL, 
        CHARINDEX('www.', Customers.WebsiteURL) + 4,
        (CHARINDEX('.com', Customers.WebsiteURL) - CHARINDEX('www.', Customers.WebsiteURL) -4)
    )
-- 2. If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies. 
SELECT s1.CustomerName
FROM Sales.Customers s1
JOIN Sales.Customers s2
ON s1.PrimaryContactPersonID = s2.CustomerID 
WHERE s1.PhoneNumber = s2.PhoneNumber

-- 3. List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
SELECT CustomerName
FROM Sales.Customers
JOIN
    (
    SELECT CustomerID
    FROM Sales.CustomerTransactions
    GROUP BY CustomerID, TransactionDate
    HAVING MAX(TransactionDate) < '2016-01-01'
    ) temp
ON Customers.CustomerID = temp.CustomerID

-- 4. List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.
SELECT StockItemName, quantity
FROM Warehouse.StockItems
JOIN
    (
    SELECT StockItemID, SUM(Quantity) as quantity
    FROM Warehouse.StockItemTransactions
        JOIN
            (
            SELECT [OrderID],[OrderDate]
            FROM [WideWorldImporters].[Sales].[Orders]
            WHERE OrderDate < '2014-01-01' AND OrderDate >= '2013-01-01'
            ) temp
    ON StockItemTransactions.PurchaseOrderID = temp.OrderID
    GROUP BY StockItemID
    ) temp2
ON StockItems.StockItemID = temp2.StockItemID

-- 5. List of stock items that have at least 10 characters in description.
SELECT StockItemName, Description
FROM Warehouse.StockItems
JOIN
    (
    SELECT StockItemID, Description
    FROM Sales.OrderLines
    GROUP BY StockItemID, Description
    HAVING LEN(Description) >= 10
    ) temp
ON StockItems.StockItemID = temp.StockItemID

-- 6. List of stock items that are not sold to the state of Alabama and Georgia in 2014.
SELECT StockItems.StockItemName
--SELECT TOP (1000) Customers.CustomerName, Orders.OrderDate, Cities.CityName, StateProvinces.StateProvinceName, StockItems.StockItemName
FROM [Sales].[Customers] Customers
JOIN [Sales].[Orders] Orders
ON Customers.CustomerID = Orders.CustomerID
JOIN [Application].[Cities] Cities
ON Cities.CityID = Customers.DeliveryCityID
JOIN [Application].[StateProvinces] StateProvinces
ON Cities.StateProvinceID = StateProvinces.StateProvinceID
JOIN [Sales].[OrderLines] OrderLines
ON OrderLines.OrderID = Orders.OrderID
JOIN [Warehouse].[StockItems] StockItems
ON StockItems.StockItemID = OrderLines.StockItemID
WHERE OrderDate >='2014-01-01' AND OrderDate <'2015-01-01' AND StateProvinces.StateProvinceName != 'Alabama' OR StateProvinces.StateProvinceName != 'Georgia'
GROUP BY StockItems.StockItemName

-- 7. List of States and Avg dates for processing (confirmed delivery date – order date).
SELECT temp.StateProvinceName, AVG(temp.diff) as Avg_dates
FROM
    (
    SELECT DATEDIFF(Day, Orders.OrderDate, Invoices.ConfirmedDeliveryTime) as diff,  Orders.OrderDate, Invoices.ConfirmedDeliveryTime,StateProvinces.StateProvinceName
    FROM [Sales].[Invoices] Invoices
    JOIN [Sales].[Orders] Orders
    ON Invoices.OrderID = Orders.OrderID

    JOIN [Sales].[Customers] Customers
    ON Customers.CustomerID = Orders.CustomerID
    JOIN [Application].[Cities] Cities
    ON Cities.CityID = Customers.DeliveryCityID
    JOIN [Application].[StateProvinces] StateProvinces
    ON Cities.StateProvinceID = StateProvinces.StateProvinceID
    ) temp
GROUP BY StateProvinceName

-- 8. List of States and Avg dates for processing (confirmed delivery date – order date) by month
SELECT temp.StateProvinceName, AVG(temp.diff) as Avg_dates, month
FROM
    (
    SELECT DATEDIFF(DAY, Orders.OrderDate, Invoices.ConfirmedDeliveryTime) as diff,  Orders.OrderDate, Invoices.ConfirmedDeliveryTime,StateProvinces.StateProvinceName, MONTH(OrderDate) as month
    FROM [Sales].[Invoices] Invoices
    JOIN [Sales].[Orders] Orders
    ON Invoices.OrderID = Orders.OrderID

    JOIN [Sales].[Customers] Customers
    ON Customers.CustomerID = Orders.CustomerID
    JOIN [Application].[Cities] Cities
    ON Cities.CityID = Customers.DeliveryCityID
    JOIN [Application].[StateProvinces] StateProvinces
    ON Cities.StateProvinceID = StateProvinces.StateProvinceID
    ) temp
GROUP BY StateProvinceName, month
Order by month

-- 9. List of StockItems that the company purchased more than sold in the year of 2015.
SELECT StockItemName
FROM [Warehouse].[StockItems]
JOIN(
    select t1.StockItemID, t1.Q1, t2.Q2
    From(
        select sum(StockItemTransactions.Quantity) Q1, InvoiceLines.StockItemID
        From [Warehouse].[StockItemTransactions]
        JOIN [Sales].[InvoiceLines] InvoiceLines
        ON InvoiceLines.InvoiceID = StockItemTransactions.InvoiceID
        WHERE StockItemTransactions.TransactionOccurredWhen >= '2015-01-01' AND StockItemTransactions.TransactionOccurredWhen < '2016-01-01'
        GROUP BY InvoiceLines.StockItemID
        ) t1
    FULL JOIN(
        select sum(StockItemTransactions.Quantity) Q2,OrderLines.StockItemID
        From [Warehouse].[StockItemTransactions]
        JOIN [Sales].[OrderLines] OrderLines
        ON OrderLines.OrderID = StockItemTransactions.PurchaseOrderID
        WHERE StockItemTransactions.TransactionOccurredWhen >= '2015-01-01' AND StockItemTransactions.TransactionOccurredWhen < '2016-01-01'
        GROUP BY OrderLines.StockItemID
        ) t2
    ON t1.StockItemID = t2.StockItemID
    WHERE ABS(q1) < q2 
    ) temp
ON temp.StockItemID = StockItems.StockItemID

-- 10. List of Customers and their phone number, together with the primary contact person’s name, to whom we did not sell more than 10  mugs (search by name) in the year 2016.
SELECT Customers.CustomerName, Customers.PhoneNumber, PrimaryContact.CustomerName as PrimaryContactName, SUM(OrderLines.Quantity) as numberOfMugs
FROM [Sales].[Customers] Customers
JOIN [Sales].[Customers] PrimaryContact
ON Customers.PrimaryContactPersonID = PrimaryContact.CustomerID
JOIN [Sales].[Orders] Orders
ON Customers.CustomerID = Orders.CustomerID
JOIN [Sales].[OrderLines] OrderLines
ON OrderLines.OrderID = Orders.OrderID
JOIN [Warehouse].[StockItems] StockItems
ON StockItems.StockItemID = OrderLines.StockItemID
WHERE StockItemName LIKE '%mug%' 
GROUP BY Customers.CustomerID, Customers.CustomerName, Customers.PhoneNumber, PrimaryContact.CustomerName, OrderLines.Quantity
HAVING SUM(OrderLines.Quantity) < 10

-- 11. List all the cities that were updated after 2015-01-01.
SELECT CityName
FROM Application.Cities
WHERE ValidFrom > '2015-01-01'

-- 12. List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, customer contact person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.
SELECT StockItems.StockItemName, Customers.DeliveryAddressLine1, Customers.DeliveryAddressLine2, States.StateProvinceName, Cities.CityName, Countries.CountryName, Customers.CustomerName, PrimaryContact.CustomerName as PrimaryContactName, Customers.PhoneNumber, SUM(OrderLines.Quantity) as Quantity
FROM [Sales].[Customers] Customers
JOIN [Sales].[Customers] PrimaryContact
ON Customers.PrimaryContactPersonID = PrimaryContact.CustomerID
JOIN [Sales].[Orders] Orders
ON Customers.CustomerID = Orders.CustomerID
JOIN [Sales].[OrderLines] OrderLines
ON OrderLines.OrderID = Orders.OrderID
JOIN [Warehouse].[StockItems] StockItems
ON StockItems.StockItemID = OrderLines.StockItemID
JOIN Application.Cities Cities
ON Customers.DeliveryCityID = Cities.CityID
JOIN Application.StateProvinces States
ON Cities.StateProvinceID = States.StateProvinceID
JOIN Application.Countries Countries
ON Countries.CountryID = States.CountryID
WHERE Orders.OrderDate = '2014-07-01'
GROUP BY StockItems.StockItemName, Customers.DeliveryAddressLine1, Customers.DeliveryAddressLine2, States.StateProvinceName, Cities.CityName, Countries.CountryName, Customers.CustomerName, PrimaryContact.CustomerName, Customers.PhoneNumber, OrderLines.Quantity

-- 13. List of stock item groups and total quantity purchased, total quantity sold, and the remaining stock quantity (quantity purchased – quantity sold)
SELECT sold.StockGroupName, sold.TotalSold, purchased.TotalPurchased, (purchased.TotalPurchased - sold.TotalSold) as RemainingStockQuantity
FROM(
    select StockGroups.StockGroupName, SUM(StockItemTransactions.Quantity) as TotalSold
    From [Warehouse].[StockItemStockGroups] StockItemStockGroups
    JOIN [Warehouse].[StockItems] StockItems
    ON StockItems.StockItemID = StockItemStockGroups.StockItemID
    JOIN Warehouse.StockGroups StockGroups
    ON StockGroups.StockGroupID = StockItemStockGroups.StockGroupID
    JOIN [Warehouse].[StockItemTransactions] StockItemTransactions
    ON StockItemTransactions.StockItemID = StockItems.StockItemID
    JOIN [Sales].[InvoiceLines] InvoiceLines
    ON InvoiceLines.InvoiceID = StockItemTransactions.InvoiceID
    GROUP BY StockGroups.StockGroupName
    ) sold
JOIN (
    select StockGroups.StockGroupName, SUM(StockItemTransactions.Quantity) as TotalPurchased
    From [Warehouse].[StockItemStockGroups] StockItemStockGroups
    JOIN [Warehouse].[StockItems] StockItems
    ON StockItems.StockItemID = StockItemStockGroups.StockItemID
    JOIN Warehouse.StockGroups StockGroups
    ON StockGroups.StockGroupID = StockItemStockGroups.StockGroupID
    JOIN [Warehouse].[StockItemTransactions] StockItemTransactions
    ON StockItemTransactions.StockItemID = StockItems.StockItemID
    JOIN [Sales].[OrderLines] OrderLines
    ON OrderLines.OrderID = StockItemTransactions.PurchaseOrderID
    GROUP BY StockGroups.StockGroupName
    ) purchased
ON sold.StockGroupName = purchased.StockGroupName

-- 14. List of Cities in the US and the stock item that the city got the most deliveries in 2016. If the city did not purchase any stock items in 2016, print “No Sales”.
SELECT temp.CityName, MAX(temp.Quantity) as MaxQuantity --CASE WHEN SUM(Orders.OrderID) = 0 OR SUM(Orders.OrderID) IS NULL THEN 'No Sales' ELSE SUM(Orders.OrderID) END
FROM
(
    SELECT Cities.CityName, StockItemID, SUM(OrderLines.Quantity) as Quantity 
    FROM [Sales].[Customers] Customers
    JOIN Application.Cities Cities
    ON Customers.DeliveryCityID = Cities.CityID
    JOIN Application.StateProvinces States
    ON Cities.StateProvinceID = States.StateProvinceID
    JOIN Application.Countries Countries
    ON Countries.CountryID = States.CountryID
    JOIN [Sales].[Orders] Orders
    ON Customers.CustomerID = Orders.CustomerID
    JOIN Sales.OrderLines
    ON OrderLines.OrderID = Orders.OrderID
    WHERE Orders.OrderDate < '2017-01-01' AND Orders.OrderDate >= '2016-01-01' AND Countries.CountryName = 'United States'
    GROUP BY Cities.CityName, StockItemID
) temp
GROUP BY temp.CityName
ORDER BY temp.CityName

-- 15. List any orders that had more than one delivery attempt (located in invoice table).
SELECT OrderID
FROM [WideWorldImporters].[Sales].[Invoices]
WHERE (SELECT COUNT(*) FROM OPENJSON(ReturnedDeliveryData, '$.Events')) > 2

-- 16. List all stock items that are manufactured in China. (Country of Manufacture)
SELECT StockItems.StockItemName, JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS Manufacture
FROM Warehouse.StockItems
WHERE JSON_VALUE(CustomFields, '$.CountryOfManufacture') = 'China'

-- 17. Total quantity of stock items sold in 2015, group by country of manufacturing.
SELECT StockItems.Manufacture, SUM(OrderLines.Quantity) as Quantity
FROM (
    SELECT StockItems.StockItemID ,JSON_VALUE(StockItems.CustomFields, '$.CountryOfManufacture') as Manufacture
    FROM Warehouse.StockItems
) StockItems
JOIN Sales.OrderLines
ON OrderLines.StockItemID = StockItems.StockItemID
JOIN Sales.Orders
ON Orders.OrderID = OrderLines.OrderID
WHERE Orders.OrderDate >= '2015-01-01' AND Orders.OrderDate < '2016-01-01'
GROUP BY StockItems.Manufacture


-- 18. Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]
GO
CREATE VIEW View18 AS
SELECT *
FROM(
    SELECT StockGroups.StockGroupName, SUM(OrderLines.Quantity) as TotalSold, OrderYear.Year
    From [Warehouse].[StockItemStockGroups] StockItemStockGroups
    JOIN [Warehouse].[StockItems] StockItems
    ON StockItems.StockItemID = StockItemStockGroups.StockItemID
    JOIN Warehouse.StockGroups StockGroups
    ON StockGroups.StockGroupID = StockItemStockGroups.StockGroupID
    JOIN [Sales].OrderLines
    ON OrderLines.StockItemID = StockItems.StockItemID
    JOIN (
        SELECT OrderID, YEAR(OrderDate) AS Year
        FROM [Sales].Orders
    ) OrderYear
    ON OrderYear.OrderID = OrderLines.OrderID
    GROUP BY StockGroups.StockGroupName, OrderYear.Year
    -- ORDER BY StockGroups.StockGroupName, OrderYear.Year-- offset 0 rows
) AS SourceTable
PIVOT
(
    SUM(TotalSold) FOR Year IN (
        [2013],
        [2014],
        [2015],
        [2016]
    )
) AS PivotTable;
;
GO
SELECT * FROM View18;

-- 19. Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10] 
GO
CREATE VIEW View19 AS
SELECT *
FROM(
    SELECT StockGroups.StockGroupName, SUM(OrderLines.Quantity) as TotalSold, OrderYear.Year
    From [Warehouse].[StockItemStockGroups] StockItemStockGroups
    JOIN [Warehouse].[StockItems] StockItems
    ON StockItems.StockItemID = StockItemStockGroups.StockItemID
    JOIN Warehouse.StockGroups StockGroups
    ON StockGroups.StockGroupID = StockItemStockGroups.StockGroupID
    JOIN [Sales].OrderLines
    ON OrderLines.StockItemID = StockItems.StockItemID
    JOIN (
        SELECT OrderID, YEAR(OrderDate) AS Year
        FROM [Sales].Orders
    ) OrderYear
    ON OrderYear.OrderID = OrderLines.OrderID
    GROUP BY StockGroups.StockGroupName, OrderYear.Year
    -- ORDER BY StockGroups.StockGroupName, OrderYear.Year-- offset 0 rows
) AS SourceTable
PIVOT
(
    SUM(TotalSold) FOR StockGroupName IN (
        [Novelty-Items],
        [Clothing],
        [Mugs],
        [T-Shirts],
        [Airline-Novelties],
        [Computing-Novelties],
        [USB-Novelties],
        [Furry-Footwear],
        [Toys],
        [Packaging-Materials]
    )
) AS PivotTable;
;
--Drop view View19
-- 20. Create a function, input: order id; return: total of that order. List invoices and use that function to attach the order total to the other fields of invoices. 
GO
CREATE FUNCTION Sales.q20(@OrderId int)
RETURNS int AS
BEGIN
    DECLARE @Total int
    SELECT @Total = (Quantity * UnitPrice) --(Quantity * UnitPrice) AS Total
    FROM Sales.OrderLines
    WHERE OrderLines.OrderID = @OrderID
    RETURN @Total
END;
GO
SELECT Invoices.*, Sales.q20(OrderID) AS Total
FROM Sales.Invoices

-- 21.	Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, that input is a date; when executed, it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id) into the new table. If a given date is already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates. 
GO
CREATE SCHEMA ods;
GO
CREATE TABLE [WideWorldImporters].[ods].[Orders] (
    OrderID int,
    OrderDate DATE,
    OrderTotal int,
    CustomerID int
);

CREATE PROCEDURE Q21 @Date Date
AS

BEGIN TRANSACTION;
    SET NOCOUNT ON;
    BEGIN TRY
        SELECT Orders.OrderID, Orders.OrderDate, (Orderlines.UnitPrice * OrderLines.Quantity) AS Total, Orders.CustomerID
        FROM Sales.Orders
        JOIN Sales.Orderlines ON Orderlines.OrderID = Orders.OrderID
        WHERE OrderDate = @Date
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        --
        ROLLBACK TRANSACTION
    END CATCH;
END

-- 22. Create a new table called ods.StockItem. It has following columns: [StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments]  ,[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table.


-- 23. Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data prior to the input date and load the order data that was placed in the next 7 days following the input date.


-- 24 ---------------------
GO
DECLARE @json NVARCHAR(MAX) = N'
{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "IsChillerStock":"0",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308",
         "LastEditedBy":"1"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}
';

INSERT INTO Warehouse.StockItems (StockItemName, SupplierID, UnitPackageID, OuterPackageID, Brand, LeadTimeDays, QuantityPerOuter,
                                    IsChillerStock, TaxRate, UnitPrice, TypicalWeightPerUnit, LastEditedBy
                                 )
    SELECT * 
    FROM OPENJSON(@json)
    WITH (
            StockItemName NVARCHAR(100) '$.PurchaseOrders[0].StockItemName',
            SupplierID int '$.PurchaseOrders[0].Supplier',
            UnitPackageID int '$.PurchaseOrders[0].UnitPackageId',
            OuterPackageID int '$.PurchaseOrders[0].OuterPackageId[0]',
            Brand NVARCHAR(50) '$.PurchaseOrders[0].Brand',
            LeadTimeDays int '$.PurchaseOrders[0].LeadTimeDays',
            QuantityPerOuter int '$.PurchaseOrders[0].QuantityPerOuter',
            IsChillerStock bit '$.PurchaseOrders[0].IsChillerStock',
            TaxRate decimal(18,3) '$.PurchaseOrders[0].TaxRate',
            UnitPrice decimal(18,2) '$.PurchaseOrders[0].UnitPrice',
            TypicalWeightPerUnit decimal(18,3) '$.PurchaseOrders[0].TypicalWeightPerUnit',
            LastEditedBy int '$.PurchaseOrders[0].LastEditedBy'
         );
        
-- 25. Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
SELECT *
FROM View19
FOR JSON PATH, ROOT('Q25')

-- 26. Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.
SELECT *
FROM View19
FOR XML PATH, ROOT('Q26')
