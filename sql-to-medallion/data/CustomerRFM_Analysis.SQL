CREATE OR ALTER PROCEDURE [dbo].[usp_CustomerAnalytics_Processing]
@DataDate DATE = NULL,
@ProcessType VARCHAR(20) = 'FULL',
@DebugMode BIT = 0,
@RetentionPeriodDays INT = 365,
@HighValueThreshold DECIMAL(15,2) = 5000.00,
@LoyaltyThreshold INT = 5,
@ChurnDays_VeryHigh INT = 180,
@ChurnDays_High INT = 90,
@ChurnDays_Medium INT = 60,
@ChurnDays_Low INT = 30
AS
BEGIN
SET NOCOUNT ON;

-- Error handling variables
DECLARE @ErrorMessage NVARCHAR(4000);
DECLARE @ErrorSeverity INT;
DECLARE @ErrorState INT;
DECLARE @ErrorLine INT;
DECLARE @ErrorProcedure NVARCHAR(200);

-- Processing variables
DECLARE @ProcessStartTime DATETIME2 = GETDATE();
DECLARE @StepStartTime DATETIME2;
DECLARE @StepName VARCHAR(100);
DECLARE @RowCount INT = 0;

-- Set default date if not provided
IF @DataDate IS NULL
    SET @DataDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

-- Logging helper table
CREATE TABLE #ProcessLog (
    LogID INT IDENTITY(1,1),
    StepName VARCHAR(100),
    StartTime DATETIME2,
    EndTime DATETIME2,
    RowCount INT,
    Message VARCHAR(1000)
);

BEGIN TRY
    -- Log start
    SET @StepName = 'Initialize';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Starting Customer Analytics processing for date: ' + CONVERT(VARCHAR(10), @DataDate, 120) + ' - Type: ' + @ProcessType);
    
    -- Temporary tables for processing
    IF OBJECT_ID('tempdb..#CustomerBase') IS NOT NULL DROP TABLE #CustomerBase;
    IF OBJECT_ID('tempdb..#TransactionSummary') IS NOT NULL DROP TABLE #TransactionSummary;
    IF OBJECT_ID('tempdb..#CustomerMetrics') IS NOT NULL DROP TABLE #CustomerMetrics;
    IF OBJECT_ID('tempdb..#CustomerSegments') IS NOT NULL DROP TABLE #CustomerSegments;
    
    -- Step 1: Extract base customer data
    SET @StepName = 'Extract Customer Base';
    SET @StepStartTime = GETDATE();
    
    CREATE TABLE #CustomerBase (
        CustomerID INT,
        CustomerName NVARCHAR(100),
        Email NVARCHAR(200),
        Phone VARCHAR(20),
        Address NVARCHAR(500),
        PostalCode VARCHAR(10),
        City NVARCHAR(100),
        State NVARCHAR(100),
        Country NVARCHAR(100),
        CustomerType VARCHAR(20),
        AccountManager INT,
        CreatedDate DATE,
        ModifiedDate DATE,
        IsActive BIT,
        LastContactDate DATE
    );
    
    -- Create index for better performance
    CREATE CLUSTERED INDEX IX_CustomerBase_CustomerID ON #CustomerBase(CustomerID);
    
    INSERT INTO #CustomerBase
    SELECT
        c.CustomerID,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        c.Email,
        c.Phone,
        a.StreetAddress,
        a.PostalCode,
        a.City,
        a.State,
        a.Country,
        c.CustomerType,
        c.AccountManagerID,
        c.CreatedDate,
        c.ModifiedDate,
        CASE WHEN c.Status = 'Active' THEN 1 ELSE 0 END AS IsActive,
        MAX(i.ContactDate) AS LastContactDate
    FROM
        dbo.Customers c
    LEFT JOIN
        dbo.CustomerAddresses a ON c.CustomerID = a.CustomerID AND a.AddressType = 'Primary'
    LEFT JOIN
        dbo.CustomerInteractions i ON c.CustomerID = i.CustomerID
    WHERE
        (@ProcessType = 'FULL' OR c.ModifiedDate >= @DataDate OR i.ContactDate >= @DataDate)
    GROUP BY
        c.CustomerID, c.FirstName, c.LastName, c.Email, c.Phone, a.StreetAddress,
        a.PostalCode, a.City, a.State, a.Country, c.CustomerType,
        c.AccountManagerID, c.CreatedDate, c.ModifiedDate, c.Status;
    
    SET @RowCount = @@ROWCOUNT;
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @RowCount, Message = 'Extracted base customer data'
    WHERE StepName = @StepName;
    
    -- Step 2: Process Transaction Summary
    SET @StepName = 'Process Transaction Summary';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Calculating transaction metrics');
    
    CREATE TABLE #TransactionSummary (
        CustomerID INT,
        TotalOrders INT,
        TotalSpent DECIMAL(15,2),
        AvgOrderValue DECIMAL(15,2),
        FirstPurchaseDate DATE,
        LastPurchaseDate DATE,
        DaysSinceLastPurchase INT,
        TopCategory VARCHAR(50),
        TopProduct VARCHAR(100),
        ReturnRate DECIMAL(5,2)
    );
    
    -- Create index for better performance
    CREATE CLUSTERED INDEX IX_TransactionSummary_CustomerID ON #TransactionSummary(CustomerID);
    
    -- First, calculate base order metrics
    WITH OrderSummary AS (
        SELECT
            o.CustomerID,
            COUNT(DISTINCT o.OrderID) AS TotalOrders,
            SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalSpent,
            MIN(o.OrderDate) AS FirstPurchaseDate,
            MAX(o.OrderDate) AS LastPurchaseDate
        FROM dbo.Orders o
        JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
        WHERE o.Status = 'Completed'
        GROUP BY o.CustomerID
    )
    SELECT 
        os.CustomerID,
        os.TotalOrders,
        os.TotalSpent,
        CASE WHEN os.TotalOrders > 0 THEN os.TotalSpent / os.TotalOrders ELSE 0 END AS AvgOrderValue,
        os.FirstPurchaseDate,
        os.LastPurchaseDate,
        DATEDIFF(DAY, os.LastPurchaseDate, @DataDate) AS DaysSinceLastPurchase,
        NULL AS TopCategory,  -- Will update later
        NULL AS TopProduct,   -- Will update later
        0 AS ReturnRate       -- Will update later
    INTO #TempOrderSummary
    FROM OrderSummary os
    WHERE os.CustomerID IN (SELECT CustomerID FROM #CustomerBase);
    
    -- Create index for the temp table
    CREATE CLUSTERED INDEX IX_TempOrderSummary_CustomerID ON #TempOrderSummary(CustomerID);
    
    -- Now insert the initial data into transaction summary
    INSERT INTO #TransactionSummary
    SELECT * FROM #TempOrderSummary;
    
    -- Update top category
    WITH ProductCategories AS (
        SELECT
            o.CustomerID,
            p.Category,
            SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS CategorySpend,
            ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) DESC) AS CategoryRank
        FROM dbo.Orders o
        JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
        JOIN dbo.Products p ON od.ProductID = p.ProductID
        WHERE o.Status = 'Completed'
        GROUP BY o.CustomerID, p.Category
    )
    UPDATE ts
    SET ts.TopCategory = pc.Category
    FROM #TransactionSummary ts
    JOIN ProductCategories pc ON ts.CustomerID = pc.CustomerID AND pc.CategoryRank = 1;
    
    -- Update top product
    WITH TopProducts AS (
        SELECT
            o.CustomerID,
            p.ProductName,
            SUM(od.Quantity) AS TotalQuantity,
            ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY SUM(od.Quantity) DESC) AS ProductRank
        FROM dbo.Orders o
        JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
        JOIN dbo.Products p ON od.ProductID = p.ProductID
        WHERE o.Status = 'Completed'
        GROUP BY o.CustomerID, p.ProductName
    )
    UPDATE ts
    SET ts.TopProduct = tp.ProductName
    FROM #TransactionSummary ts
    JOIN TopProducts tp ON ts.CustomerID = tp.CustomerID AND tp.ProductRank = 1;
    
    -- Update return rate
    WITH Returns AS (
        SELECT
            r.CustomerID,
            COUNT(DISTINCT r.ReturnID) AS TotalReturns,
            tos.TotalOrders,
            CASE 
                WHEN tos.TotalOrders > 0 THEN 
                    CAST(COUNT(DISTINCT r.ReturnID) AS DECIMAL(5,2)) / CAST(tos.TotalOrders AS DECIMAL(5,2)) * 100 
                ELSE 0 
            END AS ReturnRate
        FROM dbo.Returns r
        JOIN #TempOrderSummary tos ON r.CustomerID = tos.CustomerID
        GROUP BY r.CustomerID, tos.TotalOrders
    )
    UPDATE ts
    SET ts.ReturnRate = r.ReturnRate
    FROM #TransactionSummary ts
    JOIN Returns r ON ts.CustomerID = r.CustomerID;
    
    -- Drop the temporary table now that we're done with it
    DROP TABLE #TempOrderSummary;
    
    SET @RowCount = @@ROWCOUNT;
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @RowCount, Message = 'Calculated transaction summary metrics'
    WHERE StepName = @StepName;
    
    -- Step 3: Customer Metrics Calculation
    SET @StepName = 'Calculate Customer Metrics';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Calculating advanced customer metrics');
    
    CREATE TABLE #CustomerMetrics (
        CustomerID INT,
        CustomerLifetimeValue DECIMAL(15,2),
        RecencyScore INT,
        FrequencyScore INT,
        MonetaryScore INT,
        RFMScore INT,
        ChurnProbability DECIMAL(5,2),
        NextPurchasePropensity DECIMAL(5,2),
        LoyaltyIndex DECIMAL(5,2),
        CustomerHealth VARCHAR(20)
    );
    
    -- Create index for better performance
    CREATE CLUSTERED INDEX IX_CustomerMetrics_CustomerID ON #CustomerMetrics(CustomerID);
    
    -- First calculate RFM scores
    WITH RFMScores AS (
        SELECT
            ts.CustomerID,
            -- Recency: 5 is most recent
            NTILE(5) OVER (ORDER BY ISNULL(ts.DaysSinceLastPurchase, 999999) ASC) AS RecencyScore,
            -- Frequency: 5 is most frequent
            NTILE(5) OVER (ORDER BY ISNULL(ts.TotalOrders, 0) DESC) AS FrequencyScore,
            -- Monetary: 5 is highest value
            NTILE(5) OVER (ORDER BY ISNULL(ts.TotalSpent, 0) DESC) AS MonetaryScore
        FROM #TransactionSummary ts
    )
    SELECT
        rfm.CustomerID,
        rfm.RecencyScore,
        rfm.FrequencyScore,
        rfm.MonetaryScore,
        (rfm.RecencyScore + rfm.FrequencyScore + rfm.MonetaryScore) AS RFMScore
    INTO #TempRFM
    FROM RFMScores rfm;
    
    -- Create index for the temp table
    CREATE CLUSTERED INDEX IX_TempRFM_CustomerID ON #TempRFM(CustomerID);
    
    -- Now calculate churn probabilities and other metrics
    WITH ChurnCalculation AS (
        SELECT
            ts.CustomerID,
            -- Churn probability based on configurable parameters
            CASE
                WHEN ts.DaysSinceLastPurchase > @ChurnDays_VeryHigh THEN 0.8
                WHEN ts.DaysSinceLastPurchase > @ChurnDays_High THEN 0.5
                WHEN ts.DaysSinceLastPurchase > @ChurnDays_Medium THEN 0.3
                WHEN ts.DaysSinceLastPurchase > @ChurnDays_Low THEN 0.1
                ELSE 0.05
            END AS ChurnProbability,
            
            -- Next purchase propensity
            CASE
                WHEN ts.DaysSinceLastPurchase < @ChurnDays_Low AND ts.TotalOrders > 3 THEN 0.8
                WHEN ts.DaysSinceLastPurchase < @ChurnDays_Medium AND ts.TotalOrders > 2 THEN 0.6
                WHEN ts.DaysSinceLastPurchase < @ChurnDays_High THEN 0.4
                ELSE 0.2
            END AS NextPurchasePropensity,
            
            -- Loyalty index - a normalized score (0-1) based on order count and return rate
            CASE
                WHEN ts.TotalOrders >= @LoyaltyThreshold AND ts.ReturnRate < 10 THEN
                    (ts.TotalOrders * 0.6) + ((100 - ts.ReturnRate) * 0.4)
                ELSE
                    (ts.TotalOrders * 0.4)
            END / 10 AS LoyaltyIndex
        FROM #TransactionSummary ts
    )
    INSERT INTO #CustomerMetrics
    SELECT
        rfm.CustomerID,
        -- Customer Lifetime Value (simplified calculation using churn probability)
        ts.TotalSpent * (1 + (1 - ch.ChurnProbability)) AS CustomerLifetimeValue,
        rfm.RecencyScore,
        rfm.FrequencyScore,
        rfm.MonetaryScore,
        rfm.RFMScore,
        ch.ChurnProbability,
        ch.NextPurchasePropensity,
        ch.LoyaltyIndex,
        -- Customer Health derived from metrics
        CASE
            WHEN ch.ChurnProbability > 0.7 THEN 'At Risk'
            WHEN ch.LoyaltyIndex > 0.8 THEN 'Excellent'
            WHEN ch.LoyaltyIndex > 0.6 THEN 'Good'
            WHEN ch.LoyaltyIndex > 0.4 THEN 'Average'
            ELSE 'Needs Attention'
        END AS CustomerHealth
    FROM #TempRFM rfm
    JOIN #TransactionSummary ts ON rfm.CustomerID = ts.CustomerID
    JOIN ChurnCalculation ch ON rfm.CustomerID = ch.CustomerID;
    
    -- Drop the temporary RFM table now that we're done with it
    DROP TABLE #TempRFM;
    
    SET @RowCount = @@ROWCOUNT;
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @RowCount, Message = 'Calculated advanced customer metrics'
    WHERE StepName = @StepName;
    
    -- Step 4: Customer Segmentation
    SET @StepName = 'Customer Segmentation';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Performing customer segmentation');
    
    CREATE TABLE #CustomerSegments (
        CustomerID INT,
        ValueSegment VARCHAR(50),
        BehaviorSegment VARCHAR(50),
        LifecycleSegment VARCHAR(50),
        TargetGroup VARCHAR(50),
        MarketingRecommendation VARCHAR(MAX)
    );
    
    -- Create index for better performance
    CREATE CLUSTERED INDEX IX_CustomerSegments_CustomerID ON #CustomerSegments(CustomerID);
    
    -- Insert into segments table
    INSERT INTO #CustomerSegments
    SELECT
        cb.CustomerID,
        -- Value Segmentation
        CASE
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold THEN 'High Value'
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold * 0.5 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS ValueSegment,
        
        -- Behavior Segmentation
        CASE
            WHEN cm.RFMScore >= 13 THEN 'Champions'
            WHEN cm.RecencyScore >= 4 AND cm.FrequencyScore >= 3 THEN 'Loyal Customers'
            WHEN cm.RecencyScore >= 4 AND cm.FrequencyScore <= 2 THEN 'Potential Loyalists'
            WHEN cm.RecencyScore <= 2 AND cm.FrequencyScore >= 3 AND cm.MonetaryScore >= 3 THEN 'At Risk'
            WHEN cm.RecencyScore <= 2 AND cm.FrequencyScore <= 2 AND cm.MonetaryScore <= 2 THEN 'Hibernating'
            WHEN cm.RecencyScore <= 1 THEN 'Lost'
            ELSE 'Others'
        END AS BehaviorSegment,
        
        -- Lifecycle Segmentation
        CASE
            WHEN ts.TotalOrders = 1 AND ts.DaysSinceLastPurchase <= 30 THEN 'New Customer'
            WHEN ts.TotalOrders > 1 AND cm.ChurnProbability < 0.3 THEN 'Active'
            WHEN cm.ChurnProbability >= 0.3 AND cm.ChurnProbability < 0.7 THEN 'At Risk'
            WHEN cm.ChurnProbability >= 0.7 THEN 'Churned'
            ELSE 'Inactive'
        END AS LifecycleSegment,
        
        -- Target Group
        CASE
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold AND cm.ChurnProbability < 0.3 THEN 'VIP'
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold * 0.5 AND cm.ChurnProbability >= 0.3 THEN 'Retention Priority'
            WHEN ts.TotalOrders = 1 AND ts.DaysSinceLastPurchase <= 30 THEN 'Nurture New'
            WHEN cm.NextPurchasePropensity > 0.6 THEN 'Growth Opportunity'
            WHEN ts.ReturnRate > 20 THEN 'Service Improvement'
            ELSE 'Standard'
        END AS TargetGroup,
        
        -- Marketing Recommendation
        CASE
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold AND cm.ChurnProbability < 0.3 THEN
                'Exclusive offers, VIP events, Personal shopping assistance, Early access to new products'
            WHEN cm.CustomerLifetimeValue >= @HighValueThreshold * 0.5 AND cm.ChurnProbability >= 0.3 THEN
                'Re-engagement campaign, Loyalty rewards, Personalized recommendations based on past purchases'
            WHEN ts.TotalOrders = 1 AND ts.DaysSinceLastPurchase <= 30 THEN
                'Welcome series, Educational content, First purchase follow-up, Category exploration'
            WHEN cm.NextPurchasePropensity > 0.6 THEN
                'Cross-sell/upsell, Bundle offers based on ' + ISNULL(ts.TopCategory, 'preferred category') + ', Category expansion'
            WHEN ts.ReturnRate > 20 THEN
                'Satisfaction survey, Improved product information, Size/fit guides, Service recovery'
            ELSE
                'Standard seasonal promotions, Category newsletters, Reactivation after ' + 
                CAST(ISNULL(ts.DaysSinceLastPurchase, 0) / 30 AS VARCHAR) + ' months'
        END AS MarketingRecommendation
    FROM
        #CustomerBase cb
    JOIN
        #CustomerMetrics cm ON cb.CustomerID = cm.CustomerID
    JOIN
        #TransactionSummary ts ON cb.CustomerID = ts.CustomerID;
    
    SET @RowCount = @@ROWCOUNT;
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @RowCount, Message = 'Completed customer segmentation'
    WHERE StepName = @StepName;
    
    -- Step 5: Insert into target tables
    SET @StepName = 'Data Persistence';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Persisting data to target tables');
    
    -- Set the isolation level explicitly for data modifications
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    
    -- Customer Profile table update - using batching for improved performance
    DECLARE @BatchSize INT = 10000;
    DECLARE @BatchCount INT = 0;
    DECLARE @TotalCustomers INT = (SELECT COUNT(*) FROM #CustomerBase);
    DECLARE @LastCustomerID INT = 0;
    
    WHILE @BatchCount < @TotalCustomers
    BEGIN
        BEGIN TRANSACTION;
        
        -- MERGE statement for Customer Profile (batched)
        MERGE INTO dbo.CustomerProfiles AS target
        USING (
            SELECT TOP (@BatchSize)
                cb.CustomerID,
                cb.CustomerName,
                cb.Email,
                cb.Phone,
                cb.Address,
                cb.PostalCode,
                cb.City,
                cb.State,
                cb.Country,
                cb.CustomerType,
                cb.AccountManager,
                cb.CreatedDate,
                cb.IsActive,
                cb.LastContactDate,
                ts.TotalOrders,
                ts.TotalSpent,
                ts.AvgOrderValue,
                ts.FirstPurchaseDate,
                ts.LastPurchaseDate,
                ts.DaysSinceLastPurchase,
                ts.TopCategory,
                ts.TopProduct,
                ts.ReturnRate
            FROM #CustomerBase cb
            LEFT JOIN #TransactionSummary ts ON cb.CustomerID = ts.CustomerID
            WHERE cb.CustomerID > @LastCustomerID
            ORDER BY cb.CustomerID
        ) AS source
        ON (target.CustomerID = source.CustomerID)
        WHEN MATCHED THEN
            UPDATE SET
                target.CustomerName = source.CustomerName,
                target.Email = source.Email,
                target.Phone = source.Phone,
                target.Address = source.Address,
                target.PostalCode = source.PostalCode,
                target.City = source.City,
                target.State = source.State,
                target.Country = source.Country,
                target.CustomerType = source.CustomerType,
                target.AccountManager = source.AccountManager,
                target.IsActive = source.IsActive,
                target.LastContactDate = source.LastContactDate,
                target.TotalOrders = source.TotalOrders,
                target.TotalSpent = source.TotalSpent,
                target.AvgOrderValue = source.AvgOrderValue,
                target.FirstPurchaseDate = source.FirstPurchaseDate,
                target.LastPurchaseDate = source.LastPurchaseDate,
                target.DaysSinceLastPurchase = source.DaysSinceLastPurchase,
                target.TopCategory = source.TopCategory,
                target.TopProduct = source.TopProduct,
                target.ReturnRate = source.ReturnRate,
                target.ModifiedDate = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (
                CustomerID, CustomerName, Email, Phone, Address, PostalCode, City, State, Country,
                CustomerType, AccountManager, CreatedDate, IsActive, LastContactDate, TotalOrders,
                TotalSpent, AvgOrderValue, FirstPurchaseDate, LastPurchaseDate, DaysSinceLastPurchase,
                TopCategory, TopProduct, ReturnRate, ModifiedDate
            )
            VALUES (
                source.CustomerID, source.CustomerName, source.Email, source.Phone, source.Address,
                source.PostalCode, source.City, source.State, source.Country, source.CustomerType,
                source.AccountManager, source.CreatedDate, source.IsActive, source.LastContactDate,
                source.TotalOrders, source.TotalSpent, source.AvgOrderValue, source.FirstPurchaseDate,
                source.LastPurchaseDate, source.DaysSinceLastPurchase, source.TopCategory,
                source.TopProduct, source.ReturnRate, GETDATE()
            );
        
        SET @LastCustomerID = (SELECT MAX(CustomerID) FROM #CustomerBase WHERE CustomerID > @LastCustomerID AND CustomerID <= @LastCustomerID + @BatchSize);
        SET @BatchCount = @BatchCount + @@ROWCOUNT;
        
        COMMIT TRANSACTION;
    END
    
    -- Customer Analytics table update - replace data for the date (also batched)
    -- First delete existing records for this date
    BEGIN TRANSACTION;
    
    DELETE FROM dbo.CustomerAnalytics
    WHERE ProcessDate = @DataDate;
    
    COMMIT TRANSACTION;
    
    -- Now insert in batches
    SET @BatchCount = 0;
    SET @LastCustomerID = 0;
    
    WHILE @BatchCount < @TotalCustomers
    BEGIN
        BEGIN TRANSACTION;
        
        INSERT INTO dbo.CustomerAnalytics (
            ProcessDate, CustomerID, CustomerLifetimeValue, RecencyScore, FrequencyScore,
            MonetaryScore, RFMScore, ChurnProbability, NextPurchasePropensity,
            LoyaltyIndex, CustomerHealth, ValueSegment, BehaviorSegment,
            LifecycleSegment, TargetGroup, MarketingRecommendation
        )
        SELECT TOP (@BatchSize)
            @DataDate,
            cm.CustomerID,
            cm.CustomerLifetimeValue,
            cm.RecencyScore,
            cm.FrequencyScore,
            cm.MonetaryScore,
            cm.RFMScore,
            cm.ChurnProbability,
            cm.NextPurchasePropensity,
            cm.LoyaltyIndex,
            cm.CustomerHealth,
            cs.ValueSegment,
            cs.BehaviorSegment,
            cs.LifecycleSegment,
            cs.TargetGroup,
            cs.MarketingRecommendation
        FROM #CustomerMetrics cm
        JOIN #CustomerSegments cs ON cm.CustomerID = cs.CustomerID
        WHERE cm.CustomerID > @LastCustomerID
        ORDER BY cm.CustomerID;
        
        SET @RowCount = @@ROWCOUNT;
        SET @BatchCount = @BatchCount + @RowCount;
        SET @LastCustomerID = (SELECT MAX(CustomerID) FROM #CustomerMetrics WHERE CustomerID > @LastCustomerID AND CustomerID <= @LastCustomerID + @BatchSize);
        
        COMMIT TRANSACTION;
    END
    
    -- Cleanup data older than retention period
    IF @DebugMode = 0
    BEGIN
        BEGIN TRANSACTION;
        
        DELETE FROM dbo.CustomerAnalytics
        WHERE ProcessDate < DATEADD(DAY, -@RetentionPeriodDays, @DataDate);
        
        COMMIT TRANSACTION;
    END
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @BatchCount, Message = 'Data persistence completed'
    WHERE StepName = @StepName;
    
    -- Step 6: Generate summary report
    SET @StepName = 'Generate Summary';
    SET @StepStartTime = GETDATE();
    
    INSERT INTO #ProcessLog (StepName, StartTime, RowCount, Message)
    VALUES (@StepName, @StepStartTime, 0, 'Generating summary statistics');
    
    -- Insert into summary log
    BEGIN TRANSACTION;
    
    INSERT INTO dbo.CustomerAnalyticsSummary (
        ProcessDate,
        ProcessType,
        TotalCustomers,
        ActiveCustomers,
        NewCustomers,
        ChurningCustomers,
        HighValueCustomers,
        AverageLifetimeValue,
        TotalProcessingTimeMs,
        ProcessingStatus
    )
    SELECT
        @DataDate,
        @ProcessType,
        COUNT(DISTINCT cb.CustomerID),
        SUM(CASE WHEN cs.LifecycleSegment = 'Active' THEN 1 ELSE 0 END),
        SUM(CASE WHEN cs.LifecycleSegment = 'New Customer' THEN 1 ELSE 0 END),
        SUM(CASE WHEN cs.LifecycleSegment = 'At Risk' OR cs.LifecycleSegment = 'Churned' THEN 1 ELSE 0 END),
        SUM(CASE WHEN cs.ValueSegment = 'High Value' THEN 1 ELSE 0 END),
        AVG(cm.CustomerLifetimeValue),
        DATEDIFF(MILLISECOND, @ProcessStartTime, GETDATE()),
        'Success'
    FROM #CustomerBase cb
    JOIN #CustomerMetrics cm ON cb.CustomerID = cm.CustomerID
    JOIN #CustomerSegments cs ON cb.CustomerID = cs.CustomerID;
    
    SET @RowCount = @@ROWCOUNT;
    
    COMMIT TRANSACTION;
    
    UPDATE #ProcessLog
    SET EndTime = GETDATE(), RowCount = @RowCount, Message = 'Summary statistics generated'
    WHERE StepName = @StepName;
    
    -- Log all process steps for debugging
    IF @DebugMode = 1
    BEGIN
        SELECT * FROM #ProcessLog ORDER BY LogID;
    END
    
    -- Cleanup temporary tables
    IF OBJECT_ID('tempdb..#CustomerBase') IS NOT NULL DROP TABLE #CustomerBase;
    IF OBJECT_ID('tempdb..#TransactionSummary') IS NOT NULL DROP TABLE #TransactionSummary;
    IF OBJECT_ID('tempdb..#CustomerMetrics') IS NOT NULL DROP TABLE #CustomerMetrics;
    IF OBJECT_ID('tempdb..#CustomerSegments') IS NOT NULL DROP TABLE #CustomerSegments;
    IF OBJECT_ID('tempdb..#TempOrderSummary') IS NOT NULL DROP TABLE #TempOrderSummary;
    IF OBJECT_ID('tempdb..#TempRFM') IS NOT NULL DROP TABLE #TempRFM;
    
END TRY
BEGIN CATCH
    -- Error handling
    SELECT
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE(),
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ERROR_PROCEDURE();

    -- Rollback any open transaction
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    -- Log error to process log if it still exists
    IF OBJECT_ID('tempdb..#ProcessLog') IS NOT NULL
    BEGIN
        INSERT INTO #ProcessLog (StepName, StartTime, EndTime, RowCount, Message)
        VALUES (@StepName, @StepStartTime, GETDATE(), -1, 'ERROR: ' + @ErrorMessage);
    END
    
    -- Insert error record into summary
    INSERT INTO dbo.CustomerAnalyticsSummary (
        ProcessDate,
        ProcessType,
        TotalCustomers,
        TotalProcessingTimeMs,
        ProcessingStatus,
        ErrorMessage
    )
    VALUES (
        @DataDate,
        @ProcessType,
        0,
        DATEDIFF(MILLISECOND, @ProcessStartTime, GETDATE()),
        'Failed',
        @ErrorMessage + ' at line ' + CAST(@ErrorLine AS VARCHAR(10))
    );
    
    -- Output for debugging
    IF @DebugMode = 1 AND OBJECT_ID('tempdb..#ProcessLog') IS NOT NULL
    BEGIN
        SELECT * FROM #ProcessLog ORDER BY LogID;
    END
    
    -- Cleanup temporary tables before exiting - check if each exists first
    IF OBJECT_ID('tempdb..#CustomerBase') IS NOT NULL DROP TABLE #CustomerBase;
    IF OBJECT_ID('tempdb..#TransactionSummary') IS NOT NULL DROP TABLE #TransactionSummary;
    IF OBJECT_ID('tempdb..#CustomerMetrics') IS NOT NULL DROP TABLE #CustomerMetrics;
    IF OBJECT_ID('tempdb..#CustomerSegments') IS NOT NULL DROP TABLE #CustomerSegments;
    IF OBJECT_ID('tempdb..#TempOrderSummary') IS NOT NULL DROP TABLE #TempOrderSummary;
    IF OBJECT_ID('tempdb..#TempRFM') IS NOT NULL DROP TABLE #TempRFM;
    IF OBJECT_ID('tempdb..#ProcessLog') IS NOT NULL DROP TABLE #ProcessLog;
    
    -- Rethrow the error
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH;
END;