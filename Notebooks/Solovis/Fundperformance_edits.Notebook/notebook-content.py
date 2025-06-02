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
# META           "id": "e9fc4e80-ff69-4d45-bbdd-892592889465"
# META         },
# META         {
# META           "id": "13ef97da-5da2-466d-8c5f-2a70572c6558"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ----------- Set the namespace for the Lakehouse
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.sql("SET spark.sql.caseSensitive = TRUE")
# -----------
# SQL query
# spark.sql("Create table lh_curated.Silver.FundPerformance(FundPerformanceId Long,FundId String,ClassificationId Integer,FXRateId String,AsOfDate Date,NAVDate Date, \
# MarketValueLocal Double,MarketValueUSD Double,LastValuationAmountLocal Double,LastValuationAmountUSD Double,LastValuationDate Date,LastValuationNAVDate Date,PercentOfGIA Double,MVForwardDelta Double,\
# AdjustedMV Double,PercentAdjustedGIA Double,etlloadDateTime Date)")

# spark.sql("""Insert into lh_curated.Silver.FundPerformance(FundPerformanceId,FundId,ClassificationId,AsOfDate,NAVDate,MarketValueLocal,
#  MarketValueUSD,LastValuationAmountLocal,LastValuationAmountUSD,LastValuationDate,LastValuationNAVDate,
#  PercentOfGIA)
#  select FundPerformanceId,FundId,ClassificationId,AsOfDate,NAVDate,MarketValueLocal,
#  MarketValueUSD,LastValuationAmountLocal,LastValuationAmountUSD,LastValuationDate,LastValuationNAVDate,
#  PercentOfGIA from lh_curated.Silver.TempFundperformance""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "LakehouseSilver")
spark.sql("SET spark.sql.caseSensitive = TRUE")
#df = spark.sql("""select FundPerformanceId,FundId,ClassificationId,AsOfDate,NAVDate,MarketValueLocal from LakehouseSilver.FundPerformance """)  
                                             
                                                       
#display(df)
#spark.sql("Insert into lh_curated.Silver.FundPerformance select FundPerformanceId,FundId,ClassificationId,AsOfDate,NAVDate,MarketValueLocal,MarketValueUSD,LastValuationAmountLocal,LastValuationAmountUSD,LastValuationDate,LastValuationNAVDate,PercentOfGIA from LakehouseSilver.FundPerformance")
spark.sql("delete from lh_curated.Silver.FundPerformance where FundId = '91f3bc78-cf8d-4f09-8f6e-91dbd9a5da0a'")
spark.sql("Insert into lh_curated.Silver.FundPerformance values (595080,'91f3bc78-cf8d-4f09-8f6e-91dbd9a5da0a',275,NULL,DATE('2025-06-30'),DATE('2025-06-30'),6354180,6354180,5047211,5047211,DATE('12/20/2024'),DATE('12/31/2024'),0.000104,NULL,NULL,NULL)")			

# spark.sql("update lh_curated.Silver.FundPerformance set FundId = 'e46fbfe3-44a3-4088-9665-24f90d04f1ff'  where FundId ='212910D9-FD25-4769-90F0-7C8DBE44CAF6'")
#spark.sql("update lh_bronze.bronze.CrimsonXFundTrade set FundId = 'e46fbfe3-44a3-4088-9665-24f90d04f1ff'  where FundId ='212910D9-FD25-4769-90F0-7C8DBE44CAF6'")
# spark.sql("update lh_curated.Silver.FundPerformance set FundId = '7de35989-009e-43fe-9b01-4ad447665ff7'  where FundId ='9E9A6074-962B-4311-9875-473B34AFDFCF'")   

#Setup the currency rate data
schema = StructType([
    StructField("CurrencyId", IntegerType(), True),
    StructField("CurrencyRate", DoubleType(), True),
])
currency_data = [
(	1	,	1.0	)	,
(	2	,	0.7272992	)	,
(	3	,	0.64355	)	,
(	4	,	0.59705	)	,
(	5	,	0.0069302	)	,
(	6	,	0.0553787	)	,
(	7	,	1.34855	)	,
(	8	,	1.2172114	)	,
(	9	,	0.1521896	)	,
(	10	,	1.13525	)	,
(	11	,	0.8808632	)	,
(	12	,	0.5804504	)	,
(	13	,	0.1730688	)	,
(	14	,	0.5151585	)	,
(	15	,	0.104113	)	,
(	16	,	0.0825021	)	,
(	17	,	0.0005863	)	,
(	18	,	0.006823	)	,
(	19	,	0.0010579	)	,
(	20	,	0.0116857	)	,
(	21	,	0.0515305	)	,
(	22	,	0.0127186	)	,
(	23	,	0.7754342	)	,
(	24	,	0.0304623	)	,
(	25	,	0.0978747	)	,
(	26	,	0.1744577	)	,
(	27	,	0.2349348	)	,
(	29	,	0.1275258	)	,
(	30	,	0.0008358	)	,
(	31	,	0.0333684	)	,
(	32	,	0.1389709	)	,
(	33	,	0.2841595	)	,
(	34	,	0.0007248	)	,
(	35	,	0.00281	)	,
(	36	,	0.0254831	)	,
(	38	,	0.0115407	)	,
(	39	,	0.0075667	)	,
(	42	,	0.5586592	)	,
(	43	,	0.5879759	)	,
(	44	,	1.0	)	,
(	45	,	2.6525199	)	,
(	46	,	0.0081833	)	,
(	47	,	0.5	)	,
(	49	,	0.5	)	,
(	50	,	1.0	)	,
(	51	,	0.0116857	)	,
(	52	,	0.1447178	)	,
(	53	,	13.4498991	)	,
(	54	,	0.7754342	)	,
(	55	,	0.5791226	)	,
(	56	,	0.0003407	)	,
(	59	,	1.2195122	)	,
(	60	,	0.0017307	)	,
(	62	,	0.0095134	)	,
(	63	,	0.0002402	)	,
(	64	,	0.0023076	)	,
(	65	,	0.000349	)	,
(	66	,	0.0019721	)	,
(	67	,	0.1506739	)	,
(	68	,	0.0416667	)	,
(	69	,	0.0454836	)	,
(	71	,	0.0169463	)	,
(	72	,	0.3703704	)	,
(	73	,	0.0201126	)	,
(	74	,	0.1142857	)	,
(	76	,	0.0725558	)	,
(	77	,	0.007475	)	,
(	79	,	0.4372349	)	,
(	80	,	0.0137514	)	,
(	81	,	0.3661662	)	,
(	82	,	0.097561	)	,
(	86	,	0.1302694	)	,
(	87	,	0.0001155	)	,
(	89	,	0.0076416	)	,
(	90	,	0.0383944	)	,
(	91	,	0.0078613	)	,
(	92	,	1.3573125	)	,
(	93	,	0.0000614	)	,
(	96	,	0.0062696	)	,
(	97	,	1.409642	)	,
(	98	,	0.0019585	)	,
(	99	,	0.0077399	)	,
(	100	,	3.2589213	)	,
(	101	,	0.0114351	)	,
(	103	,	1.6152479	)	,
(	104	,	0.0000112	)	,
(	105	,	0.0553787	)	,
(	108	,	0.3287959	)	,
(	109	,	0.1238114	)	,
(	110	,	0.0184043	)	,
(	111	,	0.0002192	)	,
(	112	,	0.000577	)	,
(	113	,	0.0648508	)	,
(	115	,	0.0218531	)	,
(	116	,	0.0578369	)	,
(	117	,	0.0002797	)	,
(	118	,	0.1081198	)	,
(	119	,	0.0156482	)	,
(	121	,	0.0553787	)	,
(	122	,	0.0073035	)	,
(	124	,	0.0273043	)	,
(	125	,	0.0006293	)	,
(	127	,	2.5974026	)	,
(	128	,	0.0035464	)	,
(	130	,	1.0	)	,
(	131	,	0.2435698	)	,
(	132	,	0.0001251	)	,
(	133	,	0.2753607	)	,
(	134	,	0.0179228	)	,
(	136	,	0.2670548	)	,
(	137	,	0.2746498	)	,
(	138	,	0.2242253	)	,
(	139	,	0.0007043	)	,
(	141	,	2.7770064	)	,
(	143	,	0.2665494	)	,
(	144	,	0.0096838	)	,
(	145	,	0.0677762	)	,
(	148	,	8.3542189	)	,
(	150	,	0.0033395	)	,
(	152	,	0.0267795	)	,
(	153	,	0.0553787	)	,
(	156	,	0.0003675	)	,
(	157	,	2.3952096	)	,
(	158	,	0.1482668	)	,
(	159	,	0.3368421	)	,
(	161	,	0.272257	)	,
(	162	,	0.000275	)	,
(	164	,	0.024079	)	,
(	165	,	0.0240703	)	,
(	166	,	0.0000782	)	,
(	167	,	0.0083081	)	,
(	169	,	0.0000384	)	,
(	170	,	0.0041026	)	,
(	174	,	0.0010856	)	,
(	175	,	0.5804504	)	
]
df_currency = spark.createDataFrame(currency_data, schema=schema)
df_currency.createOrReplaceTempView("CurrencyTable")
#df_currency.show(truncate=False)
                                                     


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime,date
import pyspark.sql.functions as F
import pandas as pd

# ----------- Set the namespace for the Lakehouse
#spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_Bronze")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.sql("SET spark.sql.caseSensitive = TRUE")
# -----------
# Calculate Moveforward delta
                                  
df =  spark.sql("""WITH other_funds as(SELECT FundName,f.FundId,ft.CurrencyId,
                                        CASE WHEN FundName != 'HMC Internal - House' THEN (SUM(ftf.Amount * c.CurrencyRate))
                                        END AS MVForwardDelta
                                        FROM lh_bronze.Bronze.CrimsonXFundTrade ft
                                        JOIN lh_bronze.Bronze.CrimsonXFund f ON f.FundId = ft.FundId
                                        JOIN lh_bronze.Bronze.CrimsonXFundTradeForecastedTransaction ftf ON ftf.FundTradeId = ft.FundTradeId
                                        AND ftf.ExposureDate > CURRENT_DATE AND ft.IsDeleted = 0 AND ftf.IsDeleted = 0 AND ft.FundTradeTypeId <> 2
                                        JOIN CurrencyTable c ON c.CurrencyId = ft.CurrencyId
                                        GROUP BY f.FundName,f.FundId,ft.CurrencyId),
                  tempcalc as(
                                        SELECT SUM(oft.MVForwardDelta) mvother FROM other_funds oft)  ,
                  hmcinternal as(
                                        SELECT FundName ,f.FundId,ft.CurrencyId,
                                        CASE WHEN SUM(tc.mvother) > 0 THEN  -(SUM(ftf.Amount * c.CurrencyRate)) --there needs to be currency conversion
                                        END AS MVForwardDelta
                                        FROM lh_bronze.Bronze.CrimsonXFundTrade ft
                                        JOIN lh_bronze.Bronze.CrimsonXFund f ON f.FundId = ft.FundId
                                        JOIN lh_bronze.Bronze.CrimsonXFundTradeForecastedTransaction ftf ON ftf.FundTradeId = ft.FundTradeId
                                        AND ftf.ExposureDate > CURRENT_DATE AND ft.IsDeleted = 0 AND ftf.IsDeleted = 0 AND ft.FundTradeTypeId <> 2
                                        JOIN CurrencyTable c ON c.CurrencyId = ft.CurrencyId
                                        ,tempcalc tc 
                                        WHERE f.FundName ='HMC Internal - House'
                                        GROUP BY f.FundName,f.FundId,ft.CurrencyId)

                                        SELECT FundName,(MVForwardDelta * 1000000) as MVForwardDelta,FundId  FROM other_funds 
                                        UNION  
                                        SELECT FundName,(MVForwardDelta *1000000) as MVForwardDelta,FundId  FROM  hmcinternal 
                                        """)

navDate = pd.Timestamp(date.today()).to_period('M').end_time.strftime('%Y-%m-%d') 
print(navDate)
#display(df)

# Calculate AdjustedMV 
df2 = spark.table("lh_curated.Silver.FundPerformance") \
    .filter(F.col("NAVDate") == navDate) 
df2 = df2.drop("MVForwardDelta","AdjustedMV","PercentAdjustedGIA")
#display(df2)
df3 = df.join(df2 , on = "FundId" ,how="inner")
display(df3)

df3 = df3.withColumn("AdjustedMV", F.when(F.col("MarketValueUSD") > 0 ,F.col("MarketValueUSD") + F.col("MVForwardDelta")).otherwise(0))
#display(df3)

#Calculate totalmarketvalue for the NavDate
df4 = spark.sql("""select sum(MarketValueUSD) *1000000  as totalmarketvalue,FundId,NAVDate from lh_curated.Silver.FundPerformance where NAVDate = DATE('2025-06-30') Group by FundId,NAVDate  """ )
df3 = df3.join(df4 , on = ["FundId","NAVDate"] ,how="inner")

#display(df3)

# calculate PercentAdjustedGIA for the NavDate
df3 = df3.withColumn("PercentAdjustedGIA", F.when(F.col("AdjustedMV") > 0 ,F.col("AdjustedMV") /F.col( "totalmarketvalue")).otherwise(0))
df3 = df3.drop("totalmarketvalue")
display(df3)  

#Append to the FundPerformance table

# lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
# tableName = "FundPerformance" 

# deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 

# total_df.write.format("delta").mode("append").save(deltaTablePath)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
