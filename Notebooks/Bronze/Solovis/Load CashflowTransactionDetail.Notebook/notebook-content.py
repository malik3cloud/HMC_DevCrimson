# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "13ef97da-5da2-466d-8c5f-2a70572c6558",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced",
# META       "known_lakehouses": [
# META         {
# META           "id": "13ef97da-5da2-466d-8c5f-2a70572c6558"
# META         },
# META         {
# META           "id": "e9fc4e80-ff69-4d45-bbdd-892592889465"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# --- Function to Traverse Cash Directory to get all other cash accounts
from pyspark.sql.functions import col
from datetime import datetime,date

spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_bronze")
spark.sql("SET spark.sql.caseSensitive = TRUE")

def getCashAccounts():
    dfCashTag = spark.sql("""SELECT TagSetId, Category TagSetCategory, Id TagId, ParentId TagIdParent, Description TagDescription
                            FROM lh_bronze.Bronze.SolovisTagsets
                            WHERE Category = 'Asset Class - GIA'
                            AND   Description = 'Net Cash'  """)  # -- id = 11 

    dfAllTags = spark.sql("""SELECT TagSetId, Category TagSetCategory, Id TagId, ParentId TagIdParent, Description TagDescription
                            FROM lh_bronze.Bronze.SolovisTagsets tsi
                            WHERE tsi.Category = 'Asset Class - GIA' 
                            and   tsi.Description != 'HMC Internal - House'
                            and   NOT(tsi.Description LIKE 'HMC Internal%Offset%' OR tsi.Description LIKE 'HMC Internal%Fin. Costs%')
                            and   not tsi.Description = 'HMC Internal - SUS (Fin. Costs)'  """)  # -- id = 11 

    dfAll = dfAllTags.alias("AllTags")
    BaseTagLevel = dfCashTag.alias("base")
    # base = dfCashTag
    dfAllCashTags = BaseTagLevel
    PrevTagLevel = BaseTagLevel

    # Iteratively find indirect reports
    while True:
        prev_alias = PrevTagLevel.alias("prev")

        NextTagLevel = dfAll.join(
            prev_alias,
            col("AllTags.TagIdParent") == col("prev.TagId"),
            "inner"
        ).select(
            col("AllTags.TagSetId"),
            col("AllTags.TagSetCategory"),
            col("AllTags.TagId"),
            col("AllTags.TagIdParent"),
            col("AllTags.TagDescription")
        )
        
        NewTagRows = NextTagLevel.subtract(dfAllCashTags)  # prevent duplicates
        if NewTagRows.count() == 0:
            break
        dfAllCashTags = dfAllCashTags.union(NewTagRows)
        PrevTagLevel = NewTagRows

    return dfAllCashTags


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gather all the daily transactions active book and funds
dfInternalActiveBook =  spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, f.FundId
	, sc.ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 
	, st.TransactionTypeTagId
FROM   lh_bronze.Bronze.SolovisTransactions st
		JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.CrimsonXFund f ON upper(si.AssetClassGia) = upper(f.FundName)
		JOIN lh_bronze.Bronze.CrimsonXShareClass sc ON upper(f.FundId) = upper(sc.FundId)
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE  (si.AssetClassGia LIKE 'HMC Internal%')
			and   NOT(si.AssetClassGia LIKE 'HMC Internal%Offset%' OR si.AssetClassGia LIKE 'HMC Internal%Fin. Costs%' or si.AssetClassGia like 'HMC Internal - SUS (Notional Adj)' or si.AssetClassGia = 'HMC Internal - SUS (Fin. Costs)')
		AND    sc.IsDefault = 1
		AND st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)
UNION ALL
SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, f.FundId
	, sc.ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 
	, st.TransactionTypeTagId
FROM   lh_bronze.Bronze.SolovisTransactions st
		JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.CrimsonXFund f ON f.FundName ='HMC Internal - SUS'
		JOIN lh_bronze.Bronze.CrimsonXShareClass sc ON upper(f.FundId) = upper(sc.FundId)
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE  (si.AssetClassGia = 'HMC Internal - SUS (Fin. Costs)' 
			or si.AssetClassGia = 'HMC Internal - SUS (Notional Adj)')
		AND    sc.IsDefault = 1
		AND st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)""")

dfGeneralExpenses =  spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, f.FundId
	, sc.ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 	   
	, st.TransactionTypeTagId	
FROM   lh_bronze.Bronze.SolovisTransactions st
			JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
			JOIN lh_bronze.Bronze.SolovisEntities se ON si.HoldingId = se.Id
			JOIN lh_bronze.Bronze.CrimsonXFund f ON se.Label = f.FundName
			JOIN lh_bronze.Bronze.CrimsonXShareClass sc ON f.FundId = sc.FundId
			JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
			JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE  se.Label = 'General Expense - Absolute Return'
		AND    sc.IsDefault = 1
		AND st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)""")

dfExternalActiveBook =  spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, xref.FundId FundId
	, xref.ShareClassId ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 
	, st.TransactionTypeTagId	   
FROM   lh_bronze.Bronze.SolovisTransactions st
		JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.HMCDataWarehouseExternalActiveBook xref ON  xref.XrefType   = 'External Active Book'
			AND xref.StartDate <= CURRENT_TIMESTAMP
			AND xref.EndDate   >= CURRENT_TIMESTAMP
			AND upper(xref.AssetClassGIA) = upper(si.AssetClassGia)
			AND xref.ActionType = 'INCLUDE' 
		JOIN lh_bronze.Bronze.CrimsonXFund f ON UPPER(f.FundId) = UPPER(xref.FundIdCrimsonX)
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)""")

dfCashOffset =  spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, '6DD56666-F892-4148-B62A-D7A150AE4053' FundId
	, '79834536-1FD2-47D1-B296-ABA5EBF6163A' ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 	
	, st.TransactionTypeTagId	   
FROM   lh_bronze.Bronze.SolovisTransactions st
		JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.CrimsonXShareClass sc ON upper(sc.ShareClassId) = '79834536-1FD2-47D1-B296-ABA5EBF6163A'
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE  (si.AssetClassGia LIKE 'HMC Internal%Offset%' OR si.AssetClassGia LIKE 'HMC Internal%Fin. Costs%')
		AND NOT si.AssetClassGia = 'HMC Internal - SUS (Fin. Costs)'
		AND st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)""")

dfCashAccounts = getCashAccounts()
dfCashAccounts.createOrReplaceTempView("CashTags")

dfCashOther = spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, '6DD56666-F892-4148-B62A-D7A150AE4053' FundId
	, '52542EEE-1EED-4192-A24D-BB6DED34EF14' ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 	
	, st.TransactionTypeTagId	   
FROM CashTags ct
		JOIN lh_bronze.Bronze.SolovisInvestments si ON ct.TagId = si.AssetClassGiaTagId
		JOIN lh_bronze.Bronze.SolovisTransactions st ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.CrimsonXFund f ON f.FundName ='HMC Internal - SUS'
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = si.LocalCurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE
	-- Keep the original roots
	ct.TagDescription = 'Net Cash'
	OR (
		-- Include all except undesired internal entries
		ct.TagDescription != 'HMC Internal - House'
		AND NOT (
			ct.TagDescription LIKE 'HMC Internal%Offset%' 
			OR ct.TagDescription LIKE 'HMC Internal%Fin. Costs%'
		)
		AND ct.TagDescription != 'HMC Internal - SUS (Fin. Costs)'
	)
	AND st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)
""")

dfFunds =  spark.sql("""SELECT si.AssetClassGia
	, si.ResourceId InvestmentResourceId
	, si.Id InvestmentId
	, xf.HMCObjectIdCrimsonX FundId
	, xsc.HMCObjectIdCrimsonX ShareClassId
	, c.CurrencyId
	, st.AmountIn
	, st.AmountOut
	, st.ImpactsCommitment
	, st.IsAdjustment
	, st.IsInternalFlow
	, st.IsStartOfDay
	, st.IsStock
	, st.RecallableAmount
	, st.ExposureDate
	, st.TradeDate
	, st.SettleDate 
	, st.TransactionTypeTagId	   	
FROM lh_bronze.Bronze.SolovisTransactions st
		JOIN lh_bronze.Bronze.SolovisInvestments si ON st.InvestmentResourceId = si.ResourceId
		JOIN lh_bronze.Bronze.SolovisEntities se ON si.HoldingId = se.Id
		JOIN lh_bronze.Bronze.vwSourceReferenceFlatten_CrimsonX xsc ON xsc.ReferenceId = si.ResourceId
																		AND xsc.HMCObjectType = 'Share Class'
		JOIN lh_bronze.Bronze.vwSourceReferenceFlatten_CrimsonX xf ON xf.ReferenceId = si.HoldingId
																		AND xf.HMCObjectType = 'Fund'
		JOIN lh_bronze.Bronze.HMCDataWarehouseSolovisCurrency scur ON scur.Id = st.CurrencyId
		JOIN lh_bronze.Bronze.CrimsonXCurrency c ON c.ISOCode = scur.Code
WHERE st.ExposureDate < DATEADD(DAY, -1, CURRENT_TIMESTAMP)""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union the active book entries and funds
# Perform data type conversions and defaults for nulls
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType, BooleanType
# , DateType, StringType, IntegerType, LongType

dfTransactions = dfInternalActiveBook \
	.union(dfGeneralExpenses) \
	.union(dfExternalActiveBook) \
	.union(dfCashOffset) \
	.union(dfCashOther) \
	.union(dfFunds)

dfTransactions = dfTransactions \
    .withColumn("AmountIn", F.coalesce(col("AmountIn").cast(DecimalType(38, 2)), F.lit(0))) \
    .withColumn("AmountOut", F.coalesce(col("AmountOut").cast(DecimalType(38, 2)), F.lit(0))) \
    .withColumn("InvestmentResourceId", col("InvestmentResourceId").cast(IntegerType())) \
    .withColumn("InvestmentId", col("InvestmentId").cast(IntegerType())) \
    .withColumn("CurrencyId", col("CurrencyId").cast(IntegerType())) \
    .withColumn("RecallableAmount", F.coalesce(col("RecallableAmount").cast(DecimalType(38,2)), F.lit(0))) \
    .withColumn("ExposureDate", col("ExposureDate").cast(DateType())) \
    .withColumn("TradeDate", col("TradeDate").cast(DateType())) \
    .withColumn("SettleDate", col("SettleDate").cast(DateType())) \
    .withColumn("TransactionTypeTagId", col("TransactionTypeTagId").cast(IntegerType())) \
    .withColumn("ImpactsCommitment", col("ImpactsCommitment").cast(BooleanType())) \
    .withColumn("IsAdjustment", col("IsAdjustment").cast(BooleanType())) \
    .withColumn("IsInternalFlow", col("IsInternalFlow").cast(BooleanType())) \
    .withColumn("IsStartOfDay", col("IsStartOfDay").cast(BooleanType())) \
    .withColumn("IsStock", col("IsStock").cast(BooleanType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply Transaction Mappings
dfTransactionMappings = spark.table("lh_bronze.Bronze.HMCDataWarehouseSolovisTransactionMap")
dfCashflows = dfTransactions.join(dfTransactionMappings, dfTransactions["TransactionTypeTagId"] == dfTransactionMappings["SolovisTransactionComponentId"], "left")

dfCashflows = dfCashflows \
    .withColumn("FundedAmountLocal", 
        F.when((F.coalesce(F.col("ImpactsFunded"), F.lit(0)) == 1) 
            & (F.col("UseCommitmentImpact") == 1) 
            & (F.col("ImpactsCommitment")), F.col("AmountIn") - F.col("AmountOut")).otherwise(0)) \
    .withColumn("ReturnedAmountLocal", F.when((F.col("ImpactsReturned") == 1), F.col("AmountOut") - F.col("AmountIn")).otherwise(0)) \
    .withColumn("RecallableAmountLocal", F.when((F.col("AmountOut") != 0) \
            & (F.col("RecallableAmount") != 0), F.abs("RecallableAmount"))
            .when((F.col("AmountOut") == 0) \
                & (F.col("AmountIn") != 0) \
                & (F.col("RecallableAmount") != 0), F.col("RecallableAmount")).otherwise(0)) \
    .withColumn("MarketValueEffetLocal", F.when((F.col("ImpactsValuation") == 1), F.col("AmountIn") - F.col("AmountOut")).otherwise(0)) \
    .withColumn("UnfundedAdjustmentLocal", F.when((F.col("CashFlowTransactionTypeId") == 18), F.col("RecallableAmount")).otherwise(0))

.withColumn("CommitmentEffectLocal",PSql.when((PSql.col("FundStructure").isin ("Drawdown","Hybrid")) & (~PSql.col("CashFlowTransactionTypeID").isin (5,13,18)) , ( (((PSql.col("AmountIn") - PSql.col("AmountOut"))*-1) * PSql.col("ImpactsCommitmentInt") )- (PSql.col("RecallableAmount")) ))
                                           .when((PSql.col("FundStructure").isin ("Drawdown","Hybrid"))  & (PSql.col("CashFlowTransactionTypeID").isin (5,13)) , ( (((PSql.col("AmountIn") - PSql.col("AmountOut"))*-1) * PSql.col("ImpactsCommitmentInt") )- (PSql.col("RecallableAmount")) ))      
                                           .when( (PSql.col("CashFlowTransactionTypeID").isin (18)) , 0.00 )                                                 
                                           .otherwise(0.00) )

           

display(dfCashflows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data out to CashflowTransactionDetail

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
