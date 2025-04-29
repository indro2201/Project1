# Databricks notebook source
# MAGIC %sql
# MAGIC /*--------------------
# MAGIC LeadType Calcultaions
# MAGIC ---------------------*/
# MAGIC create or replace table kert1asur.adhoc.base_cdmcampaignuniverse_rtp
# MAGIC select distinct
# MAGIC   univ.cdmcampaignuniverseid,
# MAGIC   univ.assetid,
# MAGIC   univ.businessunitid,
# MAGIC   univ.ContractCoverageEndsOn,
# MAGIC   univ.currentcontractstatus,
# MAGIC   univ.currentsalesorderid,
# MAGIC   univ.LaborWarrantyEndsOn,
# MAGIC   univ.ownershipstartedon
# MAGIC from
# MAGIC   kert1asur.adhoc.cdmcampaignuniverse univ;
# MAGIC
# MAGIC
# MAGIC create or replace table kert1asur.adhoc.RTpricingleadtype as
# MAGIC select distinct
# MAGIC   ub.cdmcampaignuniverseid,
# MAGIC   ub.assetid,
# MAGIC   ub.businessunitid,
# MAGIC   ub.ContractCoverageEndsOn,
# MAGIC   ub.currentcontractstatus,
# MAGIC   ub.currentsalesorderid,
# MAGIC   ub.LaborWarrantyEndsOn,
# MAGIC   ub.ownershipstartedon,
# MAGIC   case
# MAGIC     when
# MAGIC       (
# MAGIC         (
# MAGIC           currentcontractstatus = 'Active'
# MAGIC           and ContractCoverageEndsOn >= getdate()
# MAGIC           and ContractCoverageEndsOn <= (getdate() + interval '6' month)
# MAGIC         )
# MAGIC         or (
# MAGIC           so.salesOrderType = 'Competitor Contract'
# MAGIC           and (
# MAGIC             ContractCoverageEndsOn <= (getdate() + INTERVAL '1' YEAR)
# MAGIC             or ContractCoverageEndsOn < getdate()
# MAGIC           )
# MAGIC         )
# MAGIC       )
# MAGIC     then
# MAGIC       'REN'
# MAGIC     when
# MAGIC       currentcontractstatus = 'Expired'
# MAGIC       and ContractCoverageEndsOn < getdate()
# MAGIC     then
# MAGIC       'WB'
# MAGIC     when
# MAGIC       ub.businessunitid = 'Lowes'
# MAGIC       and length(trim(nvl(currentsalesorderid, ''))) = 0
# MAGIC       and length(trim(nvl(currentcontractstatus, ''))) = 0
# MAGIC       and LaborWarrantyEndsOn > getdate()
# MAGIC       and ownershipstartedon >= (getdate() - interval '60' day)
# MAGIC     then
# MAGIC       'MPOS'
# MAGIC     when
# MAGIC       ub.businessunitid = 'Lowes'
# MAGIC       and length(trim(nvl(currentsalesorderid, ''))) = 0
# MAGIC       and length(trim(nvl(currentcontractstatus, ''))) = 0
# MAGIC       and LaborWarrantyEndsOn > getdate()
# MAGIC       and ownershipstartedon < (getdate() - interval '60' day)
# MAGIC     then
# MAGIC       'INW'
# MAGIC     when
# MAGIC       length(trim(nvl(currentsalesorderid, ''))) = 0
# MAGIC       and length(trim(nvl(currentcontractstatus, ''))) = 0
# MAGIC       and LaborWarrantyEndsOn > getdate()
# MAGIC     then
# MAGIC       'INW'
# MAGIC     when
# MAGIC       length(trim(nvl(currentsalesorderid, ''))) = 0
# MAGIC       and length(trim(nvl(currentcontractstatus, ''))) = 0
# MAGIC       and LaborWarrantyEndsOn <= getdate()
# MAGIC     then
# MAGIC       'OOW'
# MAGIC   end pricelead
# MAGIC from
# MAGIC   kert1asur.adhoc.base_cdmcampaignuniverse_rtp ub
# MAGIC     left join kert1asur.kernel.salesorder so
# MAGIC       on ub.currentsalesorderid = so.salesOrderId
# MAGIC       and ub.businessunitid = so.businessUnitId;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Fetch all assestids from campaign universe and fetch dealer id from SalesOrder table. Join with Asset table 
# MAGIC to fetch plc. This will be joined with marketing program data to get eligible Marketing Program for each asset.
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTPricingAssetList_tmp as
# MAGIC WITH universe AS (
# MAGIC   SELECT DISTINCT
# MAGIC     cdmcampaignuniverseid,
# MAGIC     businessunitid,
# MAGIC     assetid,
# MAGIC     currentsalesorderid
# MAGIC   FROM
# MAGIC     kert1asur.adhoc.cdmcampaignuniverse
# MAGIC ),
# MAGIC sales_orders AS (
# MAGIC   SELECT DISTINCT
# MAGIC     sal.salesOrderId,
# MAGIC     sal.actuarialReserveId,
# MAGIC     sal.contractEndsOn,
# MAGIC     sop.contractDeductibleAmount,
# MAGIC     sop.assetid,
# MAGIC     sal.businessunitid
# MAGIC   FROM
# MAGIC     kert1asur.kernel.salesorder sal
# MAGIC       INNER JOIN kert1asur.kernel.salesorderproduct sop
# MAGIC         ON sal.salesOrderId = sop.salesOrderId
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   univ.assetid,
# MAGIC   univ.currentsalesorderid,
# MAGIC   ast.businessunitid,
# MAGIC   ast.plc,
# MAGIC   ast.productclassification,
# MAGIC   ast.assetLocationId,
# MAGIC   ast.servicelocationid,
# MAGIC   ast.pricepaid,
# MAGIC   ast.msrp,
# MAGIC   so.actuarialReserveId AS dealerId,
# MAGIC   so.contractEndsOn,
# MAGIC   so.contractDeductibleAmount,
# MAGIC   rtplt.pricelead AS Pricingleadtype,
# MAGIC   rtplt.ownershipstartedon,
# MAGIC   rtplt.laborwarrantyendson
# MAGIC FROM
# MAGIC   universe univ
# MAGIC     INNER JOIN kert1asur.adhoc.rtpricingleadtype rtplt
# MAGIC       ON univ.assetid = rtplt.assetid
# MAGIC       AND univ.businessunitid = rtplt.businessunitid
# MAGIC       AND univ.cdmcampaignuniverseid = rtplt.cdmcampaignuniverseid
# MAGIC     INNER JOIN kert1asur.kernel.asset ast
# MAGIC       ON univ.assetId = ast.assetid
# MAGIC       AND univ.businessunitid = ast.businessunitid
# MAGIC     LEFT JOIN sales_orders so
# MAGIC       ON univ.currentsalesorderid = so.salesOrderId
# MAGIC       AND univ.assetid = so.assetid
# MAGIC       AND univ.businessunitid = so.businessunitid;

# COMMAND ----------

# MAGIC %sql
# MAGIC ----------------------------------------------------------------------------------
# MAGIC -- Eliminate Lowes assets of MPOS and MPOS-EXT which has null pricepaid #1236324
# MAGIC ----------------------------------------------------------------------------------
# MAGIC create or replace table kert1asur.adhoc.RTPricingAssetList as
# MAGIC select distinct
# MAGIC   t.*
# MAGIC from
# MAGIC   kert1asur.adhoc.RTPricingAssetList_tmp t
# MAGIC where
# MAGIC   t.assetid not in (
# MAGIC     select
# MAGIC       assetid
# MAGIC     from
# MAGIC       kert1asur.adhoc.RTPricingAssetList_tmp
# MAGIC     where
# MAGIC       businessunitid = 'Lowes'
# MAGIC       and Pricingleadtype in ('MPOS', 'MPOS-EXT')
# MAGIC       and pricepaid is null
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Split marketingProgramMix which is a comma seperated list. RealtimecampaignmetacampaigncodeId 
# MAGIC can be used to find which Marketing Program Ids were together
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTMktgPrgSplit
# MAGIC as
# MAGIC select distinct 
# MAGIC 	realtimecampaignmetacampaigncodeId,
# MAGIC 	businessunitid,
# MAGIC 	dealerId,
# MAGIC 	marketingProgramMix,
# MAGIC 	trim(pricingleadtype) as pricingLeadType,
# MAGIC 	MarketingProgramId,
# MAGIC 	source
# MAGIC from
# MAGIC 	(
# MAGIC 	select
# MAGIC 		*,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 1)) as pmmix1,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 2)) as pmmix2,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 3)) as pmmix3,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 4)) as pmmix4,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 5)) as pmmix5,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 6)) as pmmix6,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 7)) as pmmix7,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 8)) as pmmix8,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 9)) as pmmix9,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 10)) as pmmix10,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 11)) as pmmix11,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 12)) as pmmix12,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 13)) as pmmix13,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 14)) as pmmix14,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 15)) as pmmix15,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 16)) as pmmix16,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 17)) as pmmix17,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 18)) as pmmix18,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 19)) as pmmix19,
# MAGIC 		trim(split_part(MarketingProgramMix, ',', 20)) as pmmix20
# MAGIC 	from
# MAGIC 		kert1asur.kernel.realtimecampaignmetacampaigncode
# MAGIC   ) unpivot exclude nulls (
# MAGIC     (MarketingProgramId) for type in (
# MAGIC       (pmmix1) as P1,
# MAGIC 	(pmmix2) as P2,
# MAGIC 	(pmmix3) as P3,
# MAGIC 	(pmmix4) as P4,
# MAGIC 	(pmmix5) as P5,
# MAGIC 	(pmmix6) as P6,
# MAGIC 	(pmmix7) as P7,
# MAGIC 	(pmmix8) as P8,
# MAGIC 	(pmmix9) as P9,
# MAGIC 	(pmmix10) as P10,
# MAGIC 	(pmmix11) as P11,
# MAGIC 	(pmmix12) as P12,
# MAGIC 	(pmmix13) as P13,
# MAGIC 	(pmmix14) as P14,
# MAGIC 	(pmmix15) as P15,
# MAGIC 	(pmmix16) as P16,
# MAGIC 	(pmmix17) as P17,
# MAGIC 	(pmmix18) as P18,
# MAGIC 	(pmmix19) as P19,
# MAGIC 	(pmmix20) as P20
# MAGIC     )
# MAGIC   )
# MAGIC where
# MAGIC 	MarketingProgramId > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Join table RealTimeMarketingProgram to fetch other Marketing program columns
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTMktgPrgJoinColumns as
# MAGIC select distinct
# MAGIC   splt.source,
# MAGIC   splt.businessunitid,
# MAGIC   --splt.dealerId,
# MAGIC   splt.marketingProgramMix,
# MAGIC   splt.pricingLeadType,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType,
# MAGIC   MktPrg.realtimemarketingprogramId as marketingprogramId,
# MAGIC   MktPrg1.programName,
# MAGIC   MktPrg1.programTypeCode,
# MAGIC   MktPrg1.dealerId,
# MAGIC   MktPrg1.marketingprogramId as marketingprogramId1,
# MAGIC   MktPrg1.isdateofpurchase,
# MAGIC   MktPrg1.StartDateDelayPeriod,
# MAGIC   MktPrg1.offerPricesIncludesTax
# MAGIC from
# MAGIC   kert1asur.adhoc.RTMktgPrgSplit splt
# MAGIC     inner join kert1asur.kernel.realtimeMarketingProgram MktPrg
# MAGIC       on splt.MarketingProgramId = MktPrg.realtimemarketingprogramId
# MAGIC     left join kert1asur.kernel.MarketingProgram MktPrg1
# MAGIC       on splt.MarketingProgramId = MktPrg1.marketingprogramId;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC We have join list of assets with marketing programs and then we will decide best marketing program for that
# MAGIC assest id.
# MAGIC Age Calculation
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTPricingCalJoinAssets_tmp as
# MAGIC select
# MAGIC   distinct astlst.assetId,
# MAGIC   astlst.plc,
# MAGIC   astlst.productclassification,
# MAGIC   astlst.assetLocationId,
# MAGIC   astlst.servicelocationid,
# MAGIC   astlst.PricePaid,
# MAGIC   astlst.msrp,
# MAGIC   astlst.contractDeductibleAmount,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.offerPricesIncludesTax,
# MAGIC   astlst.ownershipstartedon,
# MAGIC   case
# MAGIC     when MktPrg.pricingLeadType in ('INW', 'MPOS')
# MAGIC     and MktPrg.isdateofpurchase = true then floor(
# MAGIC       date_diff(current_date(), astlst.ownershipstartedon) / 365
# MAGIC     )
# MAGIC     when MktPrg.pricingLeadType in ('INW', 'MPOS')
# MAGIC     and MktPrg.isdateofpurchase = false then floor(
# MAGIC       date_diff(
# MAGIC         astlst.laborwarrantyendson,
# MAGIC         date_add(astlst.ownershipstartedon,1)
# MAGIC       ) / 365
# MAGIC     )
# MAGIC     when MktPrg.pricingLeadType in ('REN') then floor(
# MAGIC       date_diff(
# MAGIC         greatest(
# MAGIC           reccon.paidThrough,
# MAGIC           date_add(reccon.contractEndsOn, 1)
# MAGIC         ),
# MAGIC         astlst.ownershipstartedon
# MAGIC       ) / 365
# MAGIC     )
# MAGIC     when MktPrg.pricingLeadType in ('OOW', 'WB') then floor(
# MAGIC       (
# MAGIC         datediff(current_date(), astlst.ownershipstartedon) + MktPrg.StartDateDelayPeriod
# MAGIC       ) / 365
# MAGIC     )
# MAGIC   end as ageinyears
# MAGIC from
# MAGIC   kert1asur.adhoc.RTPricingAssetList astlst
# MAGIC   join kert1asur.adhoc.RTMktgPrgJoinColumns MktPrg 
# MAGIC   on MktPrg.businessunitid = astlst.businessunitid --eliminates REN
# MAGIC   and MktPrg.pricingLeadType = astlst.PricingLeadType
# MAGIC   -- AND MktPrg.dealerId = astlst.dealerId
# MAGIC   left join kert1asur.adhoc.wrk_sop_recent_contract_assets reccon
# MAGIC   on astlst.assetid = reccon.assetid
# MAGIC   and astlst.currentsalesorderid = reccon.salesOrderID;
# MAGIC   -- where mktprg.marketingprogramId not in ('5037','8470'); -- Eliminate the ID for contract in box / But no hardcoding

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Limiting assets within permissible age
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTPricingCalJoinAssets as
# MAGIC select distinct
# MAGIC   cjat.*
# MAGIC from
# MAGIC   kert1asur.adhoc.RTPricingCalJoinAssets_tmp cjat
# MAGIC   left join kert1asur.kernel.lookupplcmasterxref pxref
# MAGIC   on cjat.plc = pxref.plc
# MAGIC     INNER JOIN kert1asur.kernel.lookupmaxagexref mxAge
# MAGIC       on upper(trim(nvl(decode(
# MAGIC         cjat.productclassification,
# MAGIC         'MA',
# MAGIC         'Major Appliances',
# MAGIC         'CE',
# MAGIC         'Consumer Electronics',
# MAGIC         'OutdoorCooking',
# MAGIC         'Outdoor Cooking',
# MAGIC         'SmallAppliance',
# MAGIC         'Small Appliance',
# MAGIC         'Heating & Air',
# MAGIC         'Heating and Air',
# MAGIC         cjat.productclassification
# MAGIC       ),pxref.productClassification))) = upper(trim(mxAge.productClassification))
# MAGIC where
# MAGIC   cjat.ageinyears <= mxAge.maxAge;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC If there is only one Marketing Program Id associated with assetid then that is 
# MAGIC final Marketing Program Id
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace table kert1asur.adhoc.RTMarketingProgramFinal as
# MAGIC select distinct 
# MAGIC   MktPrg.assetId,
# MAGIC   MktPrg.plc,
# MAGIC   MktPrg.productclassification,
# MAGIC   MktPrg.assetLocationId,
# MAGIC   MktPrg.servicelocationid,
# MAGIC   MktPrg.PricePaid,
# MAGIC   MktPrg.msrp,
# MAGIC   MktPrg.contractDeductibleAmount,
# MAGIC   MktPrg.ageinyears,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.ownershipstartedon,
# MAGIC   MktPrg.offerPricesIncludesTax
# MAGIC from
# MAGIC   kert1asur.adhoc.RTPricingCalJoinAssets MktPrg
# MAGIC     inner join (
# MAGIC       select
# MAGIC         assetid, source, businessunitid
# MAGIC       from
# MAGIC         kert1asur.adhoc.RTPricingCalJoinAssets
# MAGIC       group by
# MAGIC         assetid, source, businessunitid
# MAGIC       having
# MAGIC         count(distinct MarketingProgramId) = 1
# MAGIC     ) MktPrg1
# MAGIC       on MktPrg.assetId = MktPrg1.assetId;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC For remaining assets where we have more than one program id filter using Sub Program Type. For Real time as
# MAGIC per BSA only two sub program types are applicable which are 'HR' and 'BA'
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC CREATE or replace view kert1asur.adhoc.RTMktgPrgAsgnSubProgType AS
# MAGIC SELECT distinct
# MAGIC   MktPrg.assetId,
# MAGIC   MktPrg.plc,
# MAGIC   MktPrg.productclassification,
# MAGIC   MktPrg.assetLocationId,
# MAGIC   MktPrg.servicelocationid,
# MAGIC   MktPrg.PricePaid,
# MAGIC   MktPrg.msrp,
# MAGIC   MktPrg.contractDeductibleAmount,
# MAGIC   MktPrg.ageinyears,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType as OrigrogramSubType,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.offerPricesIncludesTax,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       MktPrg.programSubType = 'HR'
# MAGIC       and xref.plc IS NOT NULL
# MAGIC     THEN
# MAGIC       'HR'
# MAGIC     ELSE 'BA'
# MAGIC   END as programSubType,
# MAGIC   MktPrg.ownershipstartedon
# MAGIC FROM
# MAGIC   (
# MAGIC     select
# MAGIC       mp.*
# MAGIC     from
# MAGIC       kert1asur.adhoc.RTPricingCalJoinAssets mp
# MAGIC         LEFT JOIN kert1asur.adhoc.RTMarketingProgramFinal final --Exclude those records which are already inserted in final table
# MAGIC           ON mp.assetId = final.assetId
# MAGIC           where final.assetId is null -- added, to be tested
# MAGIC   ) MktPrg
# MAGIC     LEFT JOIN kert1asur.kernel.LookupHighRiskXref xref -- This table is used to validate High Risk program ids
# MAGIC       on xref.businessUnit = mktprg.businessunitid
# MAGIC       and xref.plc = MktPrg.plc
# MAGIC         where MktPrg.programSubType= CASE WHEN /*MktPrg.programSubType = 'HR'
# MAGIC                                      and*/ xref.plc IS NOT NULL
# MAGIC                                 THEN 'HR'  ELSE 'BA'  END;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Insert HR Records to Final table if only one record exists
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC INSERT INTO
# MAGIC   kert1asur.adhoc.RTMarketingProgramFinal (
# MAGIC     assetId,
# MAGIC     plc,
# MAGIC     productclassification,
# MAGIC     assetLocationId,
# MAGIC     servicelocationid,
# MAGIC     PricePaid,
# MAGIC     msrp,
# MAGIC     contractDeductibleAmount,
# MAGIC     ageinyears,
# MAGIC     businessunitid,
# MAGIC     dealerId,
# MAGIC     marketingProgramMix,
# MAGIC     pricingLeadType,
# MAGIC     MarketingProgramId,
# MAGIC     coPayAmount,
# MAGIC     msrpCeiling,
# MAGIC     msrpFloor,
# MAGIC     programSubType,
# MAGIC     source,
# MAGIC     programName,
# MAGIC     programTypeCode,
# MAGIC     StartDateDelayPeriod,
# MAGIC     ownershipstartedon,
# MAGIC     offerPricesIncludesTax
# MAGIC   )
# MAGIC SELECT distinct
# MAGIC   SubPrgVal.assetId,
# MAGIC   SubPrgVal.plc,
# MAGIC   SubPrgVal.productclassification,
# MAGIC   SubPrgVal.assetLocationId,
# MAGIC   SubPrgVal.servicelocationid,
# MAGIC   SubPrgVal.PricePaid,
# MAGIC   SubPrgVal.msrp,
# MAGIC   SubPrgVal.contractDeductibleAmount,
# MAGIC   SubPrgVal.ageinyears ,
# MAGIC   SubPrgVal.businessunitid,
# MAGIC   SubPrgVal.dealerId,
# MAGIC   SubPrgVal.marketingProgramMix,
# MAGIC   SubPrgVal.pricingLeadType,
# MAGIC   SubPrgVal.MarketingProgramId,
# MAGIC   SubPrgVal.coPayAmount,
# MAGIC   SubPrgVal.msrpCeiling,
# MAGIC   SubPrgVal.msrpFloor,
# MAGIC   SubPrgVal.programSubType,
# MAGIC   SubPrgVal.source,
# MAGIC   SubPrgVal.programName,
# MAGIC   SubPrgVal.programTypeCode,
# MAGIC   SubPrgVal.StartDateDelayPeriod,
# MAGIC   SubPrgVal.ownershipstartedon,
# MAGIC   SubPrgVal.offerPricesIncludesTax
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTMktgPrgAsgnSubProgType SubPrgVal
# MAGIC   INNER JOIN (
# MAGIC     SELECT
# MAGIC       assetid, source, businessunitid, 
# MAGIC       programSubType
# MAGIC     FROM
# MAGIC       kert1asur.adhoc.RTMktgPrgAsgnSubProgType
# MAGIC     WHERE
# MAGIC       programSubType = 'HR'
# MAGIC     GROUP BY
# MAGIC       assetid, source, businessunitid,
# MAGIC       programSubType
# MAGIC     HAVING
# MAGIC       count(distinct MarketingProgramId) = 1
# MAGIC   ) temp -- Fetch those records where only record having sub program type = HR is associated
# MAGIC   ON SubPrgVal.assetId = temp.assetId
# MAGIC   AND SubPrgVal.programSubType = temp.programSubType
# MAGIC   LEFT JOIN kert1asur.adhoc.RTMarketingProgramFinal final -- Exclude those records which already inserted in Final table
# MAGIC   ON SubPrgVal.assetId = final.assetId
# MAGIC   and SubPrgVal.source = final.source
# MAGIC WHERE
# MAGIC   final.assetId IS NULL
# MAGIC   and final.source is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Insert BA Records to Final table if only one record exists for BA
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC INSERT INTO kert1asur.adhoc.RTMarketingProgramFinal
# MAGIC (
# MAGIC   assetId
# MAGIC   ,plc
# MAGIC   ,productclassification
# MAGIC   ,assetLocationId
# MAGIC   ,servicelocationid
# MAGIC   ,PricePaid
# MAGIC   ,msrp
# MAGIC   ,contractDeductibleAmount
# MAGIC   ,ageinyears
# MAGIC  ,businessunitid
# MAGIC  ,dealerId
# MAGIC  ,marketingProgramMix
# MAGIC  ,pricingLeadType
# MAGIC  ,MarketingProgramId
# MAGIC  ,coPayAmount
# MAGIC  ,msrpCeiling
# MAGIC  ,msrpFloor
# MAGIC  ,programSubType
# MAGIC  ,source,
# MAGIC   programName,
# MAGIC   programTypeCode,
# MAGIC   StartDateDelayPeriod,
# MAGIC   ownershipstartedon,
# MAGIC   offerPricesIncludesTax
# MAGIC )
# MAGIC SELECT distinct 
# MAGIC    SubPrgVal.assetId
# MAGIC   ,SubPrgVal.plc
# MAGIC   ,SubPrgVal.productclassification
# MAGIC   ,SubPrgVal.assetLocationId
# MAGIC   ,SubPrgVal.servicelocationid
# MAGIC   ,SubPrgVal.PricePaid
# MAGIC   ,SubPrgVal.msrp
# MAGIC   ,SubPrgVal.contractDeductibleAmount
# MAGIC   ,SubPrgVal.ageinyears
# MAGIC 	,SubPrgVal.businessunitid
# MAGIC 	,SubPrgVal.dealerId
# MAGIC 	,SubPrgVal.marketingProgramMix
# MAGIC 	,SubPrgVal.pricingLeadType
# MAGIC 	,SubPrgVal.MarketingProgramId
# MAGIC 	,SubPrgVal.coPayAmount
# MAGIC 	,SubPrgVal.msrpCeiling
# MAGIC 	,SubPrgVal.msrpFloor
# MAGIC 	,SubPrgVal.programSubType
# MAGIC 	,SubPrgVal.source
# MAGIC   ,SubPrgVal.programName,
# MAGIC    SubPrgVal.programTypeCode,
# MAGIC    SubPrgVal.StartDateDelayPeriod,
# MAGIC    SubPrgVal.ownershipstartedon,
# MAGIC    SubPrgVal.offerPricesIncludesTax
# MAGIC FROM kert1asur.adhoc.RTMktgPrgAsgnSubProgType SubPrgVal
# MAGIC INNER JOIN (SELECT assetid, source, businessunitid, programSubType 
# MAGIC 			FROM kert1asur.adhoc.RTMktgPrgAsgnSubProgType
# MAGIC 			WHERE programSubType = 'BA'
# MAGIC 			GROUP BY assetid, source, businessunitid, programSubType
# MAGIC 			HAVING count(distinct MarketingProgramId) = 1
# MAGIC 			) temp
# MAGIC ON SubPrgVal.assetid = temp.assetid
# MAGIC LEFT JOIN kert1asur.adhoc.RTMarketingProgramFinal final
# MAGIC  ON SubPrgVal.assetId = final.assetId
# MAGIC   and SubPrgVal.source = final.source
# MAGIC WHERE
# MAGIC   final.assetId IS NULL
# MAGIC   and final.source is null
# MAGIC   and SubPrgVal.programSubType = 'BA';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Decide Marketing Program Id based on MSRP
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC -- DROP TABLE IF EXISTS kert1asur.adhoc.RTMktgPrgMSRPCal;
# MAGIC
# MAGIC CREATE or replace view kert1asur.adhoc.RTMktgPrgMSRPCal AS
# MAGIC SELECT distinct 
# MAGIC   MktPrg.assetId,
# MAGIC   MktPrg.plc,
# MAGIC   MktPrg.productclassification,
# MAGIC   MktPrg.assetLocationId,
# MAGIC   MktPrg.servicelocationid,
# MAGIC   MktPrg.PricePaid,
# MAGIC   MktPrg.msrp,
# MAGIC   MktPrg.contractDeductibleAmount,
# MAGIC   MktPrg.ageinyears,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType as OrigrogramSubType,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programSubType,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.ownershipstartedon,
# MAGIC   MktPrg.offerPricesIncludesTax
# MAGIC FROM
# MAGIC   (select mp.* from kert1asur.adhoc.RTMktgPrgAsgnSubProgType mp LEFT JOIN
# MAGIC       kert1asur.adhoc.RTMarketingProgramFinal final
# MAGIC       ON
# MAGIC         mp.assetId = final.assetId
# MAGIC         and mp.source = final.source
# MAGIC         WHERE
# MAGIC         final.assetid IS NULL 
# MAGIC         AND final.source is null) MktPrg
# MAGIC     INNER JOIN
# MAGIC       kert1asur.adhoc.RTMktgPrgJoinColumns Prg
# MAGIC       ON MktPrg.MarketingProgramId = Prg.MarketingProgramId;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Insert Records based on MSRP
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC INSERT INTO
# MAGIC   kert1asur.adhoc.RTMarketingProgramFinal (
# MAGIC     assetId,
# MAGIC     plc,
# MAGIC     productclassification,
# MAGIC     assetLocationId,
# MAGIC     servicelocationid,
# MAGIC     PricePaid,
# MAGIC     msrp,
# MAGIC     contractDeductibleAmount,
# MAGIC     ageinyears,
# MAGIC     businessunitid,
# MAGIC     dealerId,
# MAGIC     marketingProgramMix,
# MAGIC     pricingLeadType,
# MAGIC     MarketingProgramId,
# MAGIC     coPayAmount,
# MAGIC     msrpCeiling,
# MAGIC     msrpFloor,
# MAGIC     programSubType,
# MAGIC     source,
# MAGIC     programName,
# MAGIC     programTypeCode,
# MAGIC     StartDateDelayPeriod,
# MAGIC     ownershipstartedon,
# MAGIC     offerPricesIncludesTax
# MAGIC   )
# MAGIC   with msrp AS (
# MAGIC SELECT distinct
# MAGIC   msrp.assetId,
# MAGIC   msrp.plc,
# MAGIC   msrp.productclassification,
# MAGIC   msrp.assetLocationId,
# MAGIC   msrp.servicelocationid,
# MAGIC   msrp.PricePaid,
# MAGIC   msrp.msrp,
# MAGIC   msrp.contractDeductibleAmount,
# MAGIC   ageinyears,
# MAGIC   msrp.businessunitid,
# MAGIC   msrp.dealerId,
# MAGIC   msrp.marketingProgramMix,
# MAGIC   msrp.pricingLeadType,
# MAGIC   msrp.MarketingProgramId,
# MAGIC   msrp.coPayAmount,
# MAGIC   msrp.msrpCeiling,
# MAGIC   msrp.msrpFloor,
# MAGIC   msrp.programSubType,
# MAGIC   msrp.source,
# MAGIC   msrp.programName,
# MAGIC   msrp.programTypeCode,
# MAGIC   msrp.StartDateDelayPeriod,
# MAGIC   msrp.ownershipstartedon,
# MAGIC   msrp.offerPricesIncludesTax
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTMktgPrgMSRPCal msrp
# MAGIC WHERE
# MAGIC   (
# MAGIC     nvl(msrp.PricePaid,0) <> 0
# MAGIC     and msrp.PricePaid <= msrp.msrpCeiling
# MAGIC     and msrp.PricePaid >= msrp.msrpFloor
# MAGIC   )
# MAGIC   OR (
# MAGIC     nvl(msrp.PricePaid,0) = 0 and nvl(msrp.msrp,0) <> 0
# MAGIC     and msrp.msrp <= msrp.msrpCeiling
# MAGIC     and msrp.msrp >= msrp.msrpFloor
# MAGIC   ))
# MAGIC   select m1.* from msrp m1 join (select assetid, source, businessunitid from msrp group by assetid, source, businessunitid having count(distinct MarketingProgramId) = 1) m2 
# MAGIC   on m1.assetId = m2.assetId
# MAGIC   and m1.source = m2.source
# MAGIC   LEFT JOIN kert1asur.adhoc.RTMarketingProgramFinal final
# MAGIC   ON m1.assetId = final.assetId
# MAGIC   and m1.source = final.source
# MAGIC   where final.assetid IS NULL 
# MAGIC   and final.source is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*------------------------------------------------------------------------------------------
# MAGIC For remaining asset data where we have more than one program id filter using MSRP Mid Bands
# MAGIC --------------------------------------------------------------------------------------------*/
# MAGIC CREATE or replace view kert1asur.adhoc.RTMSRPbase AS
# MAGIC SELECT distinct
# MAGIC   SubPrgVal.businessunitid,
# MAGIC   SubPrgVal.assetId,
# MAGIC   SubPrgVal.source,
# MAGIC   percentile(DISTINCT msrpFloor, 0.5) as midmsrpfloorband,
# MAGIC   cast(percentile(DISTINCT msrpCeiling, 0.5) as decimal(10,2)) as midmsrpCeilingband
# MAGIC FROM kert1asur.adhoc.RTMktgPrgAsgnSubProgType SubPrgVal
# MAGIC where  SubPrgVal.businessunitID='Lowes'
# MAGIC and
# MAGIC   not exists (
# MAGIC     select
# MAGIC       1
# MAGIC     from
# MAGIC       kert1asur.adhoc.RTMarketingProgramFinal final -- Exclude those records which already inserted in Final table
# MAGIC     where
# MAGIC       SubPrgVal.assetid = final.assetid
# MAGIC       and SubPrgVal.businessunitid = final.businessunitid
# MAGIC       and SubPrgVal.source = final.source
# MAGIC   )
# MAGIC   group by SubPrgVal.businessunitid,
# MAGIC            SubPrgVal.assetid,
# MAGIC            SubPrgVal.source;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*------------------------------------------------------------------------------------------
# MAGIC For remaining asset data where we have more than one program id filter using MSRP Mid Bands
# MAGIC --------------------------------------------------------------------------------------------*/
# MAGIC CREATE or replace table kert1asur.adhoc.RTMSRP_lowes AS
# MAGIC SELECT distinct 
# MAGIC   sr.assetId,
# MAGIC   sr.plc,
# MAGIC   sr.productclassification,
# MAGIC   sr.assetLocationId,
# MAGIC   sr.servicelocationid,
# MAGIC   sr.PricePaid,
# MAGIC   sr.msrp,
# MAGIC   sr.contractDeductibleAmount,
# MAGIC   sr.ageinyears,
# MAGIC   sr.businessunitid,
# MAGIC   sr.dealerId,
# MAGIC   sr.marketingProgramMix,
# MAGIC   sr.pricingLeadType,
# MAGIC   sr.MarketingProgramId,
# MAGIC   sr.coPayAmount,
# MAGIC   sr.msrpCeiling,
# MAGIC   sr.msrpFloor,
# MAGIC   sr.programSubType as OrigrogramSubType,
# MAGIC   sr.source,
# MAGIC   sr.programSubType,
# MAGIC   sr.programName,
# MAGIC   sr.programTypeCode,
# MAGIC   sr.StartDateDelayPeriod,
# MAGIC   sr.ownershipstartedon,
# MAGIC   sr.offerPricesIncludesTax
# MAGIC FROM kert1asur.adhoc.RTMktgPrgAsgnSubProgType sr 
# MAGIC      join kert1asur.adhoc.RTMSRPbase msrpb
# MAGIC          on sr.assetid=msrpb.assetid 
# MAGIC          and sr.source = msrpb.source
# MAGIC          and sr.businessunitid = msrpb.businessunitid
# MAGIC where sr.msrpFloor=msrpb.midmsrpfloorband and cast(sr.msrpCeiling as decimal(10,2))=msrpb.midmsrpCeilingband;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Insert Records based on MSRP
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC INSERT INTO
# MAGIC   kert1asur.adhoc.RTMarketingProgramFinal (
# MAGIC     assetId,
# MAGIC     plc,
# MAGIC     productclassification,
# MAGIC     assetLocationId,
# MAGIC     servicelocationid,
# MAGIC     PricePaid,
# MAGIC     msrp,
# MAGIC     contractDeductibleAmount,
# MAGIC     ageinyears,
# MAGIC     businessunitid,
# MAGIC     dealerId,
# MAGIC     marketingProgramMix,
# MAGIC     pricingLeadType,
# MAGIC     MarketingProgramId,
# MAGIC     coPayAmount,
# MAGIC     msrpCeiling,
# MAGIC     msrpFloor,
# MAGIC     programSubType,
# MAGIC     source,
# MAGIC     programName,
# MAGIC     programTypeCode,
# MAGIC     StartDateDelayPeriod,
# MAGIC     ownershipstartedon,
# MAGIC     offerPricesIncludesTax
# MAGIC   )
# MAGIC SELECT distinct 
# MAGIC   m1.assetId,
# MAGIC   m1.plc,
# MAGIC   m1.productclassification,
# MAGIC   m1.assetLocationId,
# MAGIC   m1.servicelocationid,
# MAGIC   m1.PricePaid,
# MAGIC   m1.msrp,
# MAGIC   m1.contractDeductibleAmount,
# MAGIC   m1.ageinyears,
# MAGIC   m1.businessunitid,
# MAGIC   m1.dealerId,
# MAGIC   m1.marketingProgramMix,
# MAGIC   m1.pricingLeadType,
# MAGIC   m1.MarketingProgramId,
# MAGIC   m1.coPayAmount,
# MAGIC   m1.msrpCeiling,
# MAGIC   m1.msrpFloor,
# MAGIC   m1.programSubType,
# MAGIC   m1.source,
# MAGIC   m1.programName,
# MAGIC   m1.programTypeCode,
# MAGIC   m1.StartDateDelayPeriod,
# MAGIC   m1.ownershipstartedon,
# MAGIC   m1.offerPricesIncludesTax
# MAGIC FROM
# MAGIC  kert1asur.adhoc.RTMSRP_lowes m1 join (select assetid, source, businessunitid from kert1asur.adhoc.RTMSRP_lowes group by assetid, source, businessunitid having count(distinct MarketingProgramId) = 1) m2 
# MAGIC   on m1.assetId = m2.assetId
# MAGIC   and m1.source = m2.source;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Decide Marketing Program Id based on Copayment
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC -- DROP TABLE IF EXISTS kert1asur.adhoc.RTMktgPrgCoPayCal;
# MAGIC
# MAGIC CREATE or replace VIEW kert1asur.adhoc.RTMktgPrgCoPayCal AS
# MAGIC SELECT distinct
# MAGIC   MktPrg.assetId,
# MAGIC   MktPrg.plc,
# MAGIC   MktPrg.productclassification,
# MAGIC   MktPrg.assetLocationId,
# MAGIC   MktPrg.servicelocationid,
# MAGIC   MktPrg.PricePaid,
# MAGIC   MktPrg.msrp,
# MAGIC   MktPrg.contractDeductibleAmount,
# MAGIC   MktPrg.ageinyears,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType as OrigrogramSubType,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programSubType,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.ownershipstartedon,
# MAGIC   MktPrg.offerPricesIncludesTax
# MAGIC FROM
# MAGIC   (
# MAGIC     select
# MAGIC       mp.*
# MAGIC     from
# MAGIC       kert1asur.adhoc.RTMktgPrgMSRPCal mp
# MAGIC         LEFT JOIN kert1asur.adhoc.RTMarketingProgramFinal final
# MAGIC           ON mp.assetId = final.assetId
# MAGIC           and mp.source = final.source
# MAGIC     WHERE
# MAGIC       final.assetid IS NULL 
# MAGIC       AND final.source is null
# MAGIC   ) MktPrg
# MAGIC     INNER JOIN kert1asur.adhoc.RTMktgPrgJoinColumns Prg
# MAGIC       ON MktPrg.MarketingProgramId = Prg.MarketingProgramId;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Insert based on Copayment
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC INSERT INTO
# MAGIC   kert1asur.adhoc.RTMarketingProgramFinal (
# MAGIC     assetId,
# MAGIC     plc,
# MAGIC     productclassification,
# MAGIC     assetLocationId,
# MAGIC     servicelocationid,
# MAGIC     PricePaid,
# MAGIC     msrp,
# MAGIC     contractDeductibleAmount,
# MAGIC     businessunitid,
# MAGIC     dealerId,
# MAGIC     marketingProgramMix,
# MAGIC     pricingLeadType,
# MAGIC     MarketingProgramId,
# MAGIC     coPayAmount,
# MAGIC     msrpCeiling,
# MAGIC     msrpFloor,
# MAGIC     programSubType,
# MAGIC     source,
# MAGIC     ageinyears,
# MAGIC     programName,
# MAGIC     programTypeCode,
# MAGIC     StartDateDelayPeriod,
# MAGIC     ownershipstartedon,
# MAGIC     offerPricesIncludesTax
# MAGIC   ) with copay as (
# MAGIC     SELECT distinct 
# MAGIC       CoPay.assetId,
# MAGIC       CoPay.plc,
# MAGIC       CoPay.productclassification,
# MAGIC       CoPay.assetLocationId,
# MAGIC       CoPay.servicelocationid,
# MAGIC       CoPay.PricePaid,
# MAGIC       CoPay.msrp,
# MAGIC       CoPay.contractDeductibleAmount,
# MAGIC       CoPay.businessunitid,
# MAGIC       CoPay.dealerId,
# MAGIC       CoPay.marketingProgramMix,
# MAGIC       CoPay.pricingLeadType,
# MAGIC       CoPay.MarketingProgramId,
# MAGIC       CoPay.coPayAmount,
# MAGIC       CoPay.msrpCeiling,
# MAGIC       CoPay.msrpFloor,
# MAGIC       CoPay.programSubType,
# MAGIC       CoPay.source,
# MAGIC       copay.ageinyears,
# MAGIC       copay.programName,
# MAGIC       copay.programTypeCode,
# MAGIC       copay.StartDateDelayPeriod,
# MAGIC       CoPay.ownershipstartedon,
# MAGIC       CoPay.offerPricesIncludesTax
# MAGIC     FROM
# MAGIC       kert1asur.adhoc.RTMktgPrgCoPayCal CoPay
# MAGIC     WHERE
# MAGIC       (
# MAGIC         nvl(
# MAGIC           cast(CoPay.contractDeductibleAmount as STRING),
# MAGIC           :contractDeductibleAmount1
# MAGIC         ) = trim(CoPay.coPayAmount)
# MAGIC       )
# MAGIC       OR (
# MAGIC         "$" || cast(CoPay.contractDeductibleAmount as STRING) = :contractDeductibleAmount2
# MAGIC         and trim(CoPay.coPayAmount) = :contractDeductibleAmount3
# MAGIC       )
# MAGIC       OR (
# MAGIC         "$" || cast(CoPay.contractDeductibleAmount as STRING) = :contractDeductibleAmount3
# MAGIC         and trim(CoPay.coPayAmount) = :contractDeductibleAmount2
# MAGIC       )
# MAGIC   )
# MAGIC select
# MAGIC   c1.*
# MAGIC from copay c1 join (select assetid, source, businessunitid from copay group by assetid, source, businessunitid
# MAGIC     having
# MAGIC       count(distinct MarketingProgramId) = 1
# MAGIC   ) c2 on c1.assetId = c2.assetId
# MAGIC   and c1.source = c2.source
# MAGIC   and c1.businessunitid = c2.businessunitid;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Find Terms that could be offered for each program id
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTPricingCalTerms AS
# MAGIC SELECT distinct 
# MAGIC   MktPrg.assetId,
# MAGIC   MktPrg.plc,
# MAGIC   MktPrg.productclassification,
# MAGIC   MktPrg.assetLocationId,
# MAGIC   MktPrg.servicelocationid,
# MAGIC   MktPrg.PricePaid,
# MAGIC   MktPrg.msrp,
# MAGIC   MktPrg.contractDeductibleAmount,
# MAGIC   MktPrg.businessunitid,
# MAGIC   MktPrg.dealerId,
# MAGIC   MktPrg.marketingProgramMix,
# MAGIC   MktPrg.pricingLeadType,
# MAGIC   MktPrg.MarketingProgramId,
# MAGIC   MktPrg.coPayAmount,
# MAGIC   MktPrg.msrpCeiling,
# MAGIC   MktPrg.msrpFloor,
# MAGIC   MktPrg.programSubType,
# MAGIC   mxAge.maxAge,
# MAGIC   mm.numberOfYears,
# MAGIC   mm.multiplier,
# MAGIC   MktPrg.ageinyears,
# MAGIC   MktPrg.source,
# MAGIC   MktPrg.programName,
# MAGIC   MktPrg.programTypeCode,
# MAGIC   MktPrg.StartDateDelayPeriod,
# MAGIC   MktPrg.ownershipstartedon,
# MAGIC   MktPrg.offerPricesIncludesTax
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTMarketingProgramFinal MktPrg 
# MAGIC   INNER JOIN kert1asur.kernel.MultiYearMultiplier mm ON mm.marketingProgramId = mktprg.MarketingProgramId
# MAGIC   INNER JOIN kert1asur.kernel.lookupmaxagexref mxAge on decode(
# MAGIC     MktPrg.productclassification,
# MAGIC     'MA',
# MAGIC     'Major Appliances',
# MAGIC     'CE',
# MAGIC     'Consumer Electronics',
# MAGIC     'OutdoorCooking',
# MAGIC     'Outdoor Cooking',
# MAGIC     'SmallAppliance',
# MAGIC     'Small Appliance',
# MAGIC     'Heating & Air',
# MAGIC     'Heating and Air',
# MAGIC     MktPrg.productclassification
# MAGIC   ) = mxAge.productClassification
# MAGIC   and MktPrg.businessunitid = mxAge.businessunitid
# MAGIC   and MktPrg.PricingLeadType = mxAge.pricingLeadType
# MAGIC where
# MAGIC   mm.numberOfYears + nvl(MktPrg.ageinyears,0) < mxAge.maxAge;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Base Price
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTPricingCalBasePrice AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   (
# MAGIC     SELECT DISTINCT
# MAGIC       MktPrg.assetId,
# MAGIC       MktPrg.plc,
# MAGIC       MktPrg.ageinyears,
# MAGIC       MktPrg.productclassification,
# MAGIC       MktPrg.assetLocationId,
# MAGIC       MktPrg.servicelocationid,
# MAGIC       MktPrg.PricePaid,
# MAGIC       MktPrg.msrp,
# MAGIC       MktPrg.contractDeductibleAmount,
# MAGIC       MktPrg.businessunitid,
# MAGIC       MktPrg.dealerId,
# MAGIC       MktPrg.marketingProgramMix,
# MAGIC       MktPrg.pricingLeadType,
# MAGIC       MktPrg.MarketingProgramId,
# MAGIC       MktPrg.coPayAmount,
# MAGIC       MktPrg.msrpCeiling,
# MAGIC       MktPrg.msrpFloor,
# MAGIC       MktPrg.programSubType,
# MAGIC       MktPrg.maxAge,
# MAGIC       MktPrg.numberOfYears,
# MAGIC       MktPrg.multiplier,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           MktPrg.numberOfYears = 1
# MAGIC           and basprc.`1yrPrice` IS NOT NULL
# MAGIC           and basprc.`1yrPrice` <> 0
# MAGIC         THEN
# MAGIC           basprc.`1yrPrice`
# MAGIC         WHEN
# MAGIC           MktPrg.numberOfYears = 2
# MAGIC           and basprc.`2yrPrice` IS NOT NULL
# MAGIC           and basprc.`2yrPrice` <> 0
# MAGIC         THEN
# MAGIC           basprc.`2yrPrice`
# MAGIC         WHEN
# MAGIC           MktPrg.numberOfYears = 3
# MAGIC           and basprc.`3yrPrice` IS NOT NULL
# MAGIC           and basprc.`3yrPrice` <> 0
# MAGIC         THEN
# MAGIC           basprc.`3yrPrice`
# MAGIC         WHEN
# MAGIC           MktPrg.numberOfYears = 4
# MAGIC           and basprc.`4yrPrice` IS NOT NULL
# MAGIC           and basprc.`4yrPrice` <> 0
# MAGIC         THEN
# MAGIC           basprc.`4yrPrice`
# MAGIC         WHEN
# MAGIC           MktPrg.numberOfYears = 5
# MAGIC           and basprc.`5yrPrice` IS NOT NULL
# MAGIC           and basprc.`5yrPrice` <> 0
# MAGIC         THEN
# MAGIC           basprc.`5yrPrice`
# MAGIC         WHEN
# MAGIC           basprc.marketingProgram IS NULL
# MAGIC           AND pquot.consumerBasePrice IS NOT NULL
# MAGIC         THEN
# MAGIC           format_number(pquot.consumerBasePrice,2) * MktPrg.numberOfYears --added format_number to restore the decimal values as in source files
# MAGIC         ELSE NULL
# MAGIC       END as BasePrice,
# MAGIC       MktPrg.source,
# MAGIC       MktPrg.programName,
# MAGIC       MktPrg.programTypeCode,
# MAGIC       MktPrg.StartDateDelayPeriod,
# MAGIC       MktPrg.ownershipstartedon,
# MAGIC       MktPrg.offerPricesIncludesTax,
# MAGIC       case when basprc.marketingProgram IS NOT NULL 
# MAGIC             then 1 else 0 end as priceoverride
# MAGIC     from
# MAGIC       kert1asur.adhoc.RTPricingCalTerms MktPrg
# MAGIC         left JOIN
# MAGIC           kert1asur.kernel.lookupoverridebaseprice basprc
# MAGIC           ON MktPrg.MarketingProgramID = basprc.marketingProgram
# MAGIC         LEFT JOIN
# MAGIC           kert1asur.kernel.productquote pquot
# MAGIC           ON
# MAGIC             MktPrg.MarketingProgramID = pquot.marketingProgramId
# MAGIC             and pquot.plc = MktPrg.plc
# MAGIC             and pquot.startingProductAge = MktPrg.ageinyears
# MAGIC   )
# MAGIC where
# MAGIC   BasePrice is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Calculate Discounts
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTPricingCalDiscount AS
# MAGIC select distinct t.* from (
# MAGIC select *,
# MAGIC round(100*(1-((1-BundleOfferTotalAppliedDiscountPercent/100)*(1-OfferMultiTermDiscountPercent/100))),2) as BundleofferTotalDiscountPercent, --Compunding on BundleOfferTotalAppliedDiscountPercent & BundleOfferMultiTermDiscountPercent
# MAGIC cast((BundleOfferTotalAppliedDiscountAmount + BundleOfferMultiTermDiscountAmount) as decimal(20,2)) as BundleOfferTotalDiscountAmount--BundleOfferTotalAppliedDiscountAmount + BundleOfferMultiTermDiscountAmount
# MAGIC  from (
# MAGIC   select *,
# MAGIC round(100*(1-((1-SingleOfferTotalAppliedDiscountPercent/100)*(1-OfferMultiTermDiscountPercent/100))),2) as SingleofferTotalDiscountPercent, --Compunding on SingleOfferTotalAppliedDiscountPercent & SingleOfferMultiTermDiscountPercent
# MAGIC cast(COALESCE(SingleOfferMultiTermDiscountAmount,0) + COALESCE(SingleOfferTotalAppliedDiscountAmount,0) as decimal (20,2)) as  SingleOfferTotalDiscountAmount, --SingleOfferTotalAppliedDiscountAmount + SingleOfferMultiTermDiscountAmount
# MAGIC /***************************************
# MAGIC --Bundle Offer Discount Calculations
# MAGIC ****************************************/
# MAGIC COALESCE(OfferDiscountAPercent, 0) + COALESCE(OfferDiscountBPercent, 0) + COALESCE(OfferDiscountCPercent, 0) as BundleOfferTotalAppliedDiscountPercent,--OfferDiscountAPercent + OfferDiscountBPercent + OfferDiscountCPercent
# MAGIC cast((COALESCE(OfferDiscountAAmount, 0) + COALESCE(OfferDiscountBAmount, 0) + COALESCE(OfferDiscountCAmount, 0)) as decimal(20,2)) as BundleOfferTotalAppliedDiscountAmount,--OfferDiscountAPercent + OfferDiscountBPercent + OfferDiscountCPercent
# MAGIC cast(BasePrice - (cast((COALESCE(OfferDiscountAAmount, 0) + COALESCE(OfferDiscountBAmount, 0) + COALESCE(OfferDiscountCAmount, 0)) as decimal(20,2) )) as decimal(20,2)) AS BundleOfferPriceAfterAppliedDiscounts,--consumerBasePrice - BundleOfferTotalAppliedDiscountAmount
# MAGIC cast(((OfferMultiTermDiscountPercent/100)*cast(BasePrice - (cast((COALESCE(OfferDiscountAAmount, 0) + COALESCE(OfferDiscountBAmount, 0) + COALESCE(OfferDiscountCAmount, 0)) as decimal(20,2) )) as decimal(20,2))) as decimal(20,2)) as BundleOfferMultiTermDiscountAmount,--OfferMultiTermDiscountPercent * BundleOfferPriceAfterAppliedDiscounts
# MAGIC cast((cast(BasePrice - (cast((COALESCE(OfferDiscountAAmount, 0) + COALESCE(OfferDiscountBAmount, 0) + COALESCE(OfferDiscountCAmount, 0)) as decimal(20,2) )) as decimal(20,2))-cast(((OfferMultiTermDiscountPercent/100)*cast(BasePrice - (cast((COALESCE(OfferDiscountAAmount, 0) + COALESCE(OfferDiscountBAmount, 0) + COALESCE(OfferDiscountCAmount, 0)) as decimal(20,2) )) as decimal(20,2))) as decimal(20,2))) as decimal(20,2)) as BundleofferNetPriceAfterDiscounts--BundleOfferPriceAfterAppliedDiscounts - BundleOfferMultiTermDiscountAmount
# MAGIC  from (
# MAGIC   WITH combined_discounts AS (
# MAGIC   SELECT
# MAGIC     discountName,
# MAGIC     discountPercent,
# MAGIC     marketingProgramId,
# MAGIC     CASE
# MAGIC       WHEN discountOptionNo NOT IN (2, 14) THEN 'A'
# MAGIC       WHEN discountOptionNo IN (2, 14) THEN 'C'
# MAGIC     END AS discountType
# MAGIC   FROM
# MAGIC     kert1asur.kernel.discountoption
# MAGIC   WHERE
# MAGIC     isApplied = 'true'
# MAGIC )
# MAGIC SELECT
# MAGIC 		BasPrc.assetId,
# MAGIC 		BasPrc.plc,
# MAGIC 		BasPrc.productclassification,
# MAGIC 		BasPrc.assetLocationId,
# MAGIC 		BasPrc.servicelocationid,
# MAGIC 		BasPrc.PricePaid,
# MAGIC 		BasPrc.msrp,
# MAGIC 		BasPrc.contractDeductibleAmount,
# MAGIC 		BasPrc.businessunitid,
# MAGIC 		BasPrc.dealerId,
# MAGIC 		BasPrc.marketingProgramMix,
# MAGIC 		BasPrc.pricingLeadType,
# MAGIC 		BasPrc.MarketingProgramId,
# MAGIC 		BasPrc.coPayAmount,
# MAGIC 		BasPrc.msrpCeiling,
# MAGIC 		BasPrc.msrpFloor,
# MAGIC 		BasPrc.programSubType,
# MAGIC 		BasPrc.maxAge,
# MAGIC 		BasPrc.numberOfYears,
# MAGIC 		BasPrc.source,
# MAGIC 		BasPrc.programName,
# MAGIC   	BasPrc.programTypeCode,
# MAGIC   	BasPrc.StartDateDelayPeriod,
# MAGIC 		BasPrc.ownershipstartedon,
# MAGIC     BasPrc.offerPricesIncludesTax,
# MAGIC 		BasPrc.ageinyears,
# MAGIC 		CAST(basprc.BasePrice AS DECIMAL(20, 2)) AS BasePrice,
# MAGIC 		/***************************************
# MAGIC 		--DiscountA Applied Calculations
# MAGIC 		****************************************/				
# MAGIC     	disc1.discountName as OfferDiscountAName,
# MAGIC 		COALESCE(disc1.discountPercent, 0) OfferDiscountAPercent,
# MAGIC 		CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0)AS DECIMAL(20, 2)) as OfferDiscountAAmount,
# MAGIC 		/***************************************
# MAGIC 		--DiscountB is set to null as per S2T
# MAGIC 		****************************************/				
# MAGIC         '' AS OfferDiscountBName ,
# MAGIC         0 AS OfferDiscountBPercent, 
# MAGIC         0 AS OfferDiscountBAmount,
# MAGIC 		/***************************************
# MAGIC 		--DiscountC Bundle Discount Calculations
# MAGIC 		****************************************/
# MAGIC 		disc2.discountName as OfferDiscountCName,
# MAGIC 		COALESCE(disc2.discountPercent, 0) as OfferDiscountCPercent,
# MAGIC 		CAST(basprc.BasePrice * COALESCE(disc2.discountPercent / 100, 0) AS DECIMAL(20, 2)) as OfferDiscountCAmount,
# MAGIC 		/***************************************
# MAGIC 		--Single Offer Discount Calculations
# MAGIC 		****************************************/	
# MAGIC 		COALESCE(disc1.discountPercent, 0) as SingleOfferTotalAppliedDiscountPercent,--OfferDiscountAPercent + OfferDiscountBPercent
# MAGIC 		CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) as SingleOfferTotalAppliedDiscountAmount,--OfferDiscountAAmount + OfferDiscountBAmount
# MAGIC 		CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2)) AS SingleOfferPriceAfterAppliedDiscounts,--consumerBasePrice - SingleOfferTotalAppliedDiscountAmount
# MAGIC 		-- /***************************************
# MAGIC 		-- --Term Discounts
# MAGIC 		-- ****************************************/
# MAGIC 		case when BasPrc.priceoverride =1 then 0 else
# MAGIC 					round(100 - (basprc.multiplier/basprc.numberOfYears*100)) end  AS OfferMultiTermDiscountPercent,
# MAGIC 		CASE
# MAGIC 			when BasPrc.priceoverride =1 then 0
# MAGIC 			WHEN basprc.numberOfYears in (1,2,3,4,5)
# MAGIC 			THEN 
# MAGIC 			CAST(round(100 - (basprc.multiplier/basprc.numberOfYears*100))/100 * CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2)) AS DECIMAL(20,2))
# MAGIC         ELSE NULL
# MAGIC         END AS SingleOfferMultiTermDiscountAmount,--OfferMultiTermDiscountPercent * SingleOfferPriceAfterAppliedDiscounts
# MAGIC 		CASE
# MAGIC       WHEN basprc.numberOfYears IN (1, 2, 3, 4, 5) 
# MAGIC 			THEN 
# MAGIC 			CAST(CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2))-CAST(round(100 - (basprc.multiplier/basprc.numberOfYears*100))/100 * CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2)) AS DECIMAL(20,2)) AS DECIMAL(20,2))
# MAGIC 			ELSE
# MAGIC 			CAST(CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2))-CAST(CAST(basprc.BasePrice AS DECIMAL(20, 2)) - CAST(basprc.BasePrice * COALESCE(disc1.discountPercent / 100, 0) AS DECIMAL(20, 2)) AS DECIMAL(20, 2)) AS DECIMAL(20, 2))
# MAGIC       END AS SingleofferNetPriceAfterDiscounts--SingleOfferPriceAfterAppliedDiscounts - SingleOfferMultiTermDiscountAmount
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTPricingCalBasePrice BasPrc
# MAGIC   LEFT JOIN combined_discounts disc1 ON disc1.marketingProgramId = BasPrc.MarketingProgramId
# MAGIC   and disc1.discountType = 'A'
# MAGIC   LEFT JOIN combined_discounts disc2 ON disc2.marketingProgramId = BasPrc.MarketingProgramId
# MAGIC   and disc2.discountType = 'C'))) t;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC -------------------------------------------------------------------------
# MAGIC --------Fetch all the addresses from standard table for tax calculation
# MAGIC -------------------------------------------------------------------------
# MAGIC drop table if exists kert1asur.adhoc.RTContactPointAddressStdbatchpricing;
# MAGIC create table kert1asur.adhoc.RTContactPointAddressStdbatchpricing
# MAGIC select distinct contactPointId,
# MAGIC                 locality1,
# MAGIC                 region1,
# MAGIC                 postalCode
# MAGIC from Kert1asur.kernel.ContactPointAddressStd;
# MAGIC
# MAGIC -------------------------------------------------------------------------
# MAGIC --------Fetch all the addresses from ContactPointAddress which are not
# MAGIC --------available in ContactPointAddressStd for tax calculation
# MAGIC -------------------------------------------------------------------------
# MAGIC  
# MAGIC drop table if exists kert1asur.adhoc.RTContactPointAddressbatchpricing;
# MAGIC create table kert1asur.adhoc.RTContactPointAddressbatchpricing
# MAGIC select distinct cpa.contactPointId,
# MAGIC                 cpa.cityName as locality1,
# MAGIC                 cpa.stateProvinceCode as region1,
# MAGIC                 cpa.postalCode
# MAGIC from Kert1asur.kernel.ContactPointAddress cpa
# MAGIC left join  Kert1asur.adhoc.RTContactPointAddressStdbatchpricing cpastd
# MAGIC           on cpa.contactPointId =cpastd.contactPointId
# MAGIC where cpastd.contactPointId is null;
# MAGIC
# MAGIC
# MAGIC -------------------------------------------------------------------------
# MAGIC --------Union of above tables for the further processes
# MAGIC -------------------------------------------------------------------------
# MAGIC  
# MAGIC drop table if exists kert1asur.adhoc.RTpricingcpaddress;
# MAGIC create table kert1asur.adhoc.RTpricingcpaddress
# MAGIC select * from kert1asur.adhoc.RTContactPointAddressStdbatchpricing
# MAGIC union all
# MAGIC select * from kert1asur.adhoc.RTContactPointAddressbatchpricing;
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC To calculate tax we have fetch it from salestaxxref table
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTPricingRulesCalculateTax AS
# MAGIC select distinct
# MAGIC   t.*
# MAGIC from
# MAGIC   (
# MAGIC     WITH max_tax AS (
# MAGIC       SELECT
# MAGIC         UPPER(TRIM(stateProvinceCode)) as stateProvinceCode,
# MAGIC         MAX(totalSalesTax) AS max_tax
# MAGIC       FROM
# MAGIC         (select stateprovincecode, cityName, postalcode, totalSalesTax from (
# MAGIC             select UPPER(TRIM(stateprovincecode)) as stateprovincecode, UPPER(TRIM(cityName)) as cityName, TRIM(postalcode) as postalcode,totalSalesTax, row_number() over (partition by  UPPER(TRIM(stateprovincecode)), UPPER(TRIM(cityName)), TRIM(postalcode) order by totalSalesTax desc, systemModifiedTimestamp desc ) rn from kert1asur.kernel.salestaxxref ) )
# MAGIC       GROUP BY
# MAGIC         stateProvinceCode
# MAGIC     ),
# MAGIC     satx AS (
# MAGIC       SELECT distinct
# MAGIC         totalSalesTax,
# MAGIC         UPPER(TRIM(cityName)) as cityName,
# MAGIC         UPPER(TRIM(stateProvinceCode)) as stateProvinceCode,
# MAGIC         postalCode
# MAGIC       FROM
# MAGIC         (select stateprovincecode, cityName, postalcode, totalSalesTax from (
# MAGIC             select UPPER(TRIM(stateprovincecode)) as stateprovincecode, UPPER(TRIM(cityName)) as cityName, TRIM(postalcode) as postalcode,totalSalesTax, row_number() over (partition by  UPPER(TRIM(stateprovincecode)), UPPER(TRIM(cityName)), TRIM(postalcode) order by totalSalesTax desc, systemModifiedTimestamp desc ) rn from kert1asur.kernel.salestaxxref ) )
# MAGIC     ),
# MAGIC     cpadr as (
# MAGIC       select distinct
# MAGIC         cpa.contactPointId,
# MAGIC         -- UPPER(TRIM(cpa.cityName)) as cityName,
# MAGIC         UPPER(TRIM(cpa.stateProvinceCode)) as stateProvinceCode,
# MAGIC         SUBSTRING(cpa.postalCode, 1, 5)  as postalCode
# MAGIC       from
# MAGIC         Kert1asur.kernel.ContactPointAddress cpa
# MAGIC           join kert1asur.adhoc.RTPricingCalDiscount discnt
# MAGIC             ON cpa.contactPointId = nvl(discnt.servicelocationid, discnt.assetLocationId)
# MAGIC     )
# MAGIC     SELECT
# MAGIC       discnt.assetId,
# MAGIC       discnt.plc,
# MAGIC       discnt.ageinyears,
# MAGIC       discnt.productclassification,
# MAGIC       discnt.assetLocationId,
# MAGIC       discnt.servicelocationid,
# MAGIC       discnt.businessunitid,
# MAGIC       discnt.dealerId,
# MAGIC       discnt.programName,
# MAGIC       discnt.programTypeCode,
# MAGIC       discnt.StartDateDelayPeriod,
# MAGIC       discnt.marketingProgramMix,
# MAGIC       discnt.pricingLeadType,
# MAGIC       discnt.MarketingProgramId,
# MAGIC       discnt.coPayAmount,
# MAGIC       discnt.msrpCeiling,
# MAGIC       discnt.msrpFloor,
# MAGIC       discnt.programSubType,
# MAGIC       discnt.maxAge,
# MAGIC       discnt.numberOfYears,
# MAGIC       discnt.source,
# MAGIC       discnt.BasePrice,
# MAGIC       discnt.ownershipstartedon,
# MAGIC       discnt.OfferDiscountAName,
# MAGIC       discnt.OfferDiscountAPercent,
# MAGIC       discnt.OfferDiscountAAmount,
# MAGIC       discnt.OfferDiscountBName,
# MAGIC       discnt.OfferDiscountBPercent,
# MAGIC       discnt.OfferDiscountBAmount,
# MAGIC       discnt.OfferDiscountCName,
# MAGIC       discnt.OfferDiscountCPercent,
# MAGIC       discnt.OfferDiscountCAmount,
# MAGIC       discnt.SingleOfferTotalAppliedDiscountPercent,
# MAGIC       discnt.SingleOfferTotalAppliedDiscountAmount,
# MAGIC       discnt.SingleOfferPriceAfterAppliedDiscounts,
# MAGIC       discnt.OfferMultiTermDiscountPercent,
# MAGIC       discnt.SingleOfferMultiTermDiscountAmount,
# MAGIC       discnt.SingleofferNetPriceAfterDiscounts,
# MAGIC       discnt.SingleofferTotalDiscountPercent,
# MAGIC       discnt.SingleOfferTotalDiscountAmount,
# MAGIC       discnt.BundleOfferTotalAppliedDiscountPercent,
# MAGIC       discnt.BundleOfferTotalAppliedDiscountAmount,
# MAGIC       discnt.BundleOfferPriceAfterAppliedDiscounts,
# MAGIC       discnt.BundleOfferMultiTermDiscountAmount,
# MAGIC       discnt.BundleofferNetPriceAfterDiscounts,
# MAGIC       discnt.BundleofferTotalDiscountPercent,
# MAGIC       discnt.BundleOfferTotalDiscountAmount,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.SingleOfferNetPriceAfterDiscounts * (satx.totalSalesTax / 100) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NULL
# MAGIC           AND mxtx.stateProvinceCode IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(discnt.SingleOfferNetPriceAfterDiscounts * (mxtx.max_tax / 100) AS DECIMAL(20, 2))
# MAGIC         ELSE 0
# MAGIC       END AS SingleOfferTaxAmount,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.BundleOfferNetPriceAfterDiscounts * (satx.totalSalesTax / 100) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NULL
# MAGIC           AND mxtx.stateProvinceCode IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(discnt.BundleOfferNetPriceAfterDiscounts * (mxtx.max_tax / 100) AS DECIMAL(20, 2))
# MAGIC         ELSE 0
# MAGIC       END AS BundleOfferTaxAmount,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.SingleOfferNetPriceAfterDiscounts
# MAGIC             * (1 + (satx.totalSalesTax / 100)) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NULL
# MAGIC           AND mxtx.stateProvinceCode IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.SingleOfferNetPriceAfterDiscounts * (1 + (mxtx.max_tax / 100)) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         ELSE discnt.SingleOfferNetPriceAfterDiscounts
# MAGIC       END AS SingleOfferYouPayPrice,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.BundleOfferNetPriceAfterDiscounts
# MAGIC             * (1 + (satx.totalSalesTax / 100)) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         WHEN
# MAGIC           satx.totalSalesTax IS NULL
# MAGIC           AND mxtx.stateProvinceCode IS NOT NULL
# MAGIC           AND discnt.offerPricesIncludesTax = FALSE
# MAGIC         THEN
# MAGIC           CAST(
# MAGIC             discnt.BundleOfferNetPriceAfterDiscounts * (1 + (mxtx.max_tax / 100)) AS DECIMAL(20, 2)
# MAGIC           )
# MAGIC         ELSE discnt.BundleOfferNetPriceAfterDiscounts
# MAGIC       END AS BundleOfferYouPayPrice,
# MAGIC       -- cpadr.cityName,
# MAGIC       -- cpadr.stateProvinceCode,
# MAGIC       cpadr.postalCode
# MAGIC     FROM
# MAGIC       kert1asur.adhoc.RTPricingCalDiscount discnt
# MAGIC         INNER JOIN cpadr
# MAGIC           ON cpadr.contactPointId = nvl(discnt.servicelocationid, discnt.assetLocationId)
# MAGIC         LEFT JOIN satx
# MAGIC           ON  cpadr.postalCode = satx.postalCode
# MAGIC           -- AND cpadr.cityName = satx.cityName
# MAGIC           AND cpadr.stateProvinceCode = satx.stateProvinceCode
# MAGIC         LEFT JOIN max_tax mxtx
# MAGIC           ON UPPER(TRIM(cpadr.stateProvinceCode)) = mxtx.stateProvinceCode
# MAGIC         where cpadr.postalCode is not null
# MAGIC   ) t;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------------------------------------------------------------
# MAGIC Payment option is selected from table paymentoption
# MAGIC ----------------------------------------------------------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTPricingFieldCalPaymentOption_tmp AS
# MAGIC select
# MAGIC   distinct t.*
# MAGIC from
# MAGIC   (
# MAGIC     SELECT
# MAGIC       CalTax.assetId,
# MAGIC       CalTax.plc,
# MAGIC       CalTax.ageinyears,
# MAGIC       CalTax.productclassification,
# MAGIC       CalTax.assetLocationId,
# MAGIC       CalTax.businessunitid,
# MAGIC       CalTax.source,
# MAGIC       CalTax.dealerId,
# MAGIC       CalTax.programName,
# MAGIC       CalTax.programTypeCode,
# MAGIC       CalTax.StartDateDelayPeriod,
# MAGIC       CalTax.marketingProgramMix,
# MAGIC       CalTax.pricingLeadType,
# MAGIC       CalTax.MarketingProgramId,
# MAGIC       CalTax.coPayAmount,
# MAGIC       CalTax.msrpCeiling,
# MAGIC       CalTax.msrpFloor,
# MAGIC       CalTax.programSubType,
# MAGIC       CalTax.maxAge,
# MAGIC       CalTax.numberOfYears,
# MAGIC       CalTax.BasePrice,
# MAGIC       CalTax.ownershipstartedon,
# MAGIC       CalTax.OfferDiscountAName,
# MAGIC       CalTax.OfferDiscountAPercent,
# MAGIC       CalTax.OfferDiscountAAmount,
# MAGIC       CalTax.OfferDiscountBName,
# MAGIC       CalTax.OfferDiscountBPercent,
# MAGIC       CalTax.OfferDiscountBAmount,
# MAGIC       CalTax.OfferDiscountCName,
# MAGIC       CalTax.OfferDiscountCPercent,
# MAGIC       CalTax.OfferDiscountCAmount,
# MAGIC       CalTax.SingleOfferTotalAppliedDiscountPercent,
# MAGIC       CalTax.SingleOfferTotalAppliedDiscountAmount,
# MAGIC       CalTax.SingleOfferPriceAfterAppliedDiscounts,
# MAGIC       CalTax.OfferMultiTermDiscountPercent,
# MAGIC       CalTax.SingleOfferMultiTermDiscountAmount,
# MAGIC       CalTax.SingleofferNetPriceAfterDiscounts,
# MAGIC       CalTax.SingleofferTotalDiscountPercent,
# MAGIC       CalTax.SingleOfferTotalDiscountAmount,
# MAGIC       CalTax.SingleOfferTaxAmount,
# MAGIC       CalTax.SingleOfferYouPayPrice,
# MAGIC       CASE
# MAGIC         WHEN po.paymentOptionCode = 'EVERGREEN' THEN 'EVERGREEN'
# MAGIC         WHEN po.paymentOptionCode = 'MonToMonth' THEN 'MonToMonth'
# MAGIC         WHEN po.paymentOptionCode = '1Md+CvgDay' THEN '1Md+CvgDay'
# MAGIC         WHEN po.paymentOptionCode = '2Md+30Aftr' THEN '2Md+30Aftr'
# MAGIC         WHEN CalTax.SingleOfferYouPayPrice < 50 THEN 'SINGLEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 50
# MAGIC           and CalTax.SingleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           'THREEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 50
# MAGIC           and CalTax.SingleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           'SINGLEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'FourPay'
# MAGIC         THEN
# MAGIC           'FourPay'
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           'THREEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           'SINGLEPAY'
# MAGIC       END AS SingleOfferNumberOfPayments,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'EVERGREEN'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           round(((CalTax.SingleOfferYouPayPrice / 12) * 4), 2)
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'MonToMonth'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           CAST((CalTax.SingleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = '1Md+CvgDay'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           CAST((CalTax.SingleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = '2Md+30Aftr'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           round(((CalTax.SingleOfferYouPayPrice / 12) * 2), 2)
# MAGIC         WHEN CalTax.SingleOfferYouPayPrice < 50 THEN CalTax.SingleOfferYouPayPrice
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 50
# MAGIC           and CalTax.SingleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           round(CalTax.SingleOfferYouPayPrice / 3, 2)
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 50
# MAGIC           and CalTax.SingleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           CalTax.SingleOfferYouPayPrice
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'FourPay'
# MAGIC         THEN
# MAGIC           round(CalTax.SingleOfferYouPayPrice / 4, 2)
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           round(CalTax.SingleOfferYouPayPrice / 3, 2)
# MAGIC         WHEN
# MAGIC           CalTax.SingleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           CalTax.SingleOfferYouPayPrice
# MAGIC       END AS SingleOfferDownPaymentAmount,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'EVERGREEN'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           CAST((CalTax.SingleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         ELSE NULL
# MAGIC       END AS SingleOfferMonthlyRecurringPayment,
# MAGIC       CASE
# MAGIC         WHEN po.paymentOptionCode = 'EVERGREEN' THEN 1
# MAGIC         ELSE NULL
# MAGIC       END AS SingleOfferFrequencyOfPayments,
# MAGIC       CalTax.BundleOfferTotalAppliedDiscountPercent,
# MAGIC       CalTax.BundleOfferTotalAppliedDiscountAmount,
# MAGIC       CalTax.BundleOfferPriceAfterAppliedDiscounts,
# MAGIC       CalTax.BundleOfferMultiTermDiscountAmount,
# MAGIC       CalTax.BundleofferNetPriceAfterDiscounts,
# MAGIC       CalTax.BundleofferTotalDiscountPercent,
# MAGIC       CalTax.BundleOfferTotalDiscountAmount,
# MAGIC       CalTax.BundleOfferTaxAmount,
# MAGIC       CalTax.BundleOfferYouPayPrice,
# MAGIC       case
# MAGIC         WHEN po.paymentOptionCode in ('EVERGREEN','MonToMonth','1Md+CvgDay','2Md+30Aftr') then 'Y'
# MAGIC         else 'N'
# MAGIC       end as IsEvergreen, --add this
# MAGIC       CASE
# MAGIC         WHEN po.paymentOptionCode = 'EVERGREEN' THEN 'EVERGREEN'
# MAGIC         WHEN po.paymentOptionCode = 'MonToMonth' THEN 'MonToMonth'
# MAGIC         WHEN po.paymentOptionCode = '1Md+CvgDay' THEN '1Md+CvgDay'
# MAGIC         WHEN po.paymentOptionCode = '2Md+30Aftr' THEN '2Md+30Aftr'
# MAGIC         WHEN CalTax.BundleOfferYouPayPrice < 50 THEN 'SINGLEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 50
# MAGIC           and CalTax.BundleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           'THREEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 50
# MAGIC           and CalTax.BundleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           'SINGLEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'FourPay'
# MAGIC         THEN
# MAGIC           'FourPay'
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           'THREEPAY'
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           'SINGLEPAY'
# MAGIC       END AS BundleOfferNumberOfPayments,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'EVERGREEN'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           round((CalTax.BundleOfferYouPayPrice / 12) * 4, 2)
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'MonToMonth'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           CAST((CalTax.BundleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = '1Md+CvgDay'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           CAST((CalTax.BundleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = '2Md+30Aftr'
# MAGIC           AND CalTax.numberOfYears = 1
# MAGIC         THEN
# MAGIC           round(((CalTax.BundleOfferYouPayPrice / 12) * 2), 2)
# MAGIC         WHEN CalTax.BundleOfferYouPayPrice < 50 THEN CalTax.BundleOfferYouPayPrice
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 50
# MAGIC           and CalTax.BundleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           round(CalTax.BundleOfferYouPayPrice / 3, 2)
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 50
# MAGIC           and CalTax.BundleOfferYouPayPrice <= 149.99
# MAGIC           AND po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           CalTax.BundleOfferYouPayPrice
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'FourPay'
# MAGIC         THEN
# MAGIC           round(CalTax.BundleOfferYouPayPrice / 4, 2)
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'THREEPAY'
# MAGIC         THEN
# MAGIC           round(CalTax.BundleOfferYouPayPrice / 3, 2)
# MAGIC         WHEN
# MAGIC           CalTax.BundleOfferYouPayPrice >= 150
# MAGIC           and po.paymentOptionCode = 'SINGLEPAY'
# MAGIC         THEN
# MAGIC           CalTax.BundleOfferYouPayPrice
# MAGIC       END AS BundleOfferDownPaymentAmount,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           po.paymentOptionCode = 'EVERGREEN'
# MAGIC         THEN
# MAGIC           CAST((CalTax.BundleOfferYouPayPrice / 12) AS DECIMAL(20, 2))
# MAGIC         ELSE NULL
# MAGIC       END AS BundleOfferMonthlyRecurringPayment,
# MAGIC       CASE
# MAGIC         WHEN po.paymentOptionCode = 'EVERGREEN' THEN 1
# MAGIC         ELSE NULL
# MAGIC       END AS BundleOfferFrequencyOfPayments
# MAGIC     FROM
# MAGIC       kert1asur.adhoc.RTPricingRulesCalculateTax CalTax
# MAGIC         INNER JOIN kert1asur.kernel.paymentoption po
# MAGIC           ON po.marketingProgramId = CalTax.MarketingProgramId
# MAGIC   ) t
# MAGIC WHERE
# MAGIC   (
# MAGIC     t.BundleOfferYouPayPrice > 0
# MAGIC     or t.SingleOfferYouPayPrice > 0
# MAGIC   )
# MAGIC   and (
# MAGIC     t.SingleOfferNumberOfPayments is not null
# MAGIC     or t.BundleOfferNumberOfPayments is not null
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE view kert1asur.adhoc.RTPricing_NonEvergreen AS
# MAGIC SELECT distinct
# MAGIC   t.*,
# MAGIC   '' as SoEgDpTax,
# MAGIC   '' as BoEgDpTax,
# MAGIC   '' as BoEgMonTax,
# MAGIC   '' as SoEgMonTax,
# MAGIC   '' as EgNoOfPmntsDown,
# MAGIC   '' as EgInitCovPeriod
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTPricingFieldCalPaymentOption_tmp t
# MAGIC where t.IsEvergreen='N';
# MAGIC
# MAGIC CREATE OR REPLACE view kert1asur.adhoc.RTPricing_Evergreen AS
# MAGIC SELECT distinct
# MAGIC   t.*,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       t.SingleOfferNumberOfPayments = 'EVERGREEN'
# MAGIC     THEN
# MAGIC       cast((SingleOfferTaxAmount / 12) * 4 AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.SingleOfferNumberOfPayments = 'MonToMonth'
# MAGIC     THEN
# MAGIC       cast((SingleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.SingleOfferNumberOfPayments = '1Md+CvgDay'
# MAGIC     THEN
# MAGIC       cast((SingleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.SingleOfferNumberOfPayments = '2Md+30Aftr'
# MAGIC     THEN
# MAGIC       cast((SingleOfferTaxAmount / 12) * 2 AS DECIMAL(10, 2))
# MAGIC   END as SoEgDpTax,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       t.BundleOfferNumberOfPayments = 'EVERGREEN'
# MAGIC     THEN
# MAGIC       cast((BundleOfferTaxAmount / 12) * 4 AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.BundleOfferNumberOfPayments = 'MonToMonth'
# MAGIC     THEN
# MAGIC       cast((BundleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.BundleOfferNumberOfPayments = '1Md+CvgDay'
# MAGIC     THEN
# MAGIC       cast((BundleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC     WHEN
# MAGIC       t.BundleOfferNumberOfPayments = '2Md+30Aftr'
# MAGIC     THEN
# MAGIC       cast((BundleOfferTaxAmount / 12) * 2 AS DECIMAL(10, 2))
# MAGIC   END as BoEgDpTax,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       t.BundleOfferNumberOfPayments in ('EVERGREEN', 'MonToMonth', '1Md+CvgDay', '2Md+30Aftr')
# MAGIC     THEN
# MAGIC       cast((BundleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC   END as BoEgMonTax,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       t.SingleOfferNumberOfPayments in ('EVERGREEN', 'MonToMonth', '1Md+CvgDay', '2Md+30Aftr')
# MAGIC     THEN
# MAGIC       cast((SingleOfferTaxAmount / 12) AS DECIMAL(10, 2))
# MAGIC   END as SoEgMonTax,
# MAGIC   CASE
# MAGIC     WHEN t.SingleOfferNumberOfPayments = 'EVERGREEN' THEN 4
# MAGIC     WHEN t.SingleOfferNumberOfPayments = 'MonToMonth' THEN 1
# MAGIC     WHEN t.SingleOfferNumberOfPayments = '1Md+CvgDay' THEN 1
# MAGIC     WHEN t.SingleOfferNumberOfPayments = '2Md+30Aftr' THEN 2
# MAGIC   END as EgNoOfPmntsDown,
# MAGIC   CASE
# MAGIC     WHEN t.SingleOfferNumberOfPayments = 'EVERGREEN' THEN 4
# MAGIC     WHEN t.SingleOfferNumberOfPayments = 'MonToMonth' THEN 1
# MAGIC     WHEN t.SingleOfferNumberOfPayments = '1Md+CvgDay' THEN 1
# MAGIC     WHEN t.SingleOfferNumberOfPayments = '2Md+30Aftr' THEN 2
# MAGIC   END as EgInitCovPeriod
# MAGIC FROM
# MAGIC   kert1asur.adhoc.RTPricingFieldCalPaymentOption_tmp t
# MAGIC where t.IsEvergreen='Y' and t.numberOfYears = 1;
# MAGIC
# MAGIC
# MAGIC create or replace table kert1asur.adhoc.RTPricingFieldCalPaymentOption as
# MAGIC select
# MAGIC   *
# MAGIC from kert1asur.adhoc.RTPricing_NonEvergreen
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from kert1asur.adhoc.RTPricing_Evergreen;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------
# MAGIC Final table is created with those records for which we have term to offer, base price is caluclated
# MAGIC discounts are available, Tax is calculated and payment option is derrived
# MAGIC ------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTCdmPricingFieldCalculation_CWO_DEALER 
# MAGIC AS
# MAGIC SELECT distinct
# MAGIC   prf.assetId,
# MAGIC   prf.businessunitid,
# MAGIC   prf.plc,
# MAGIC   prf.BasePrice as consumerBasePrice,
# MAGIC   prf.MarketingProgramId as marketingProgramID,
# MAGIC   prf.numberOfYears as OfferTerm,
# MAGIC   prf.coPayAmount,
# MAGIC   prf.ageinyears,
# MAGIC   false as DateOfPurchaseCoverage,
# MAGIC   false as ExcludeTax,
# MAGIC   prf.programName,
# MAGIC   prf.programTypeCode,
# MAGIC   prf.StartDateDelayPeriod,
# MAGIC   prf.dealerId,
# MAGIC   prf.OfferDiscountAName,
# MAGIC   prf.OfferDiscountAPercent,
# MAGIC   prf.OfferDiscountAAmount,
# MAGIC   prf.OfferDiscountBName,
# MAGIC   prf.OfferDiscountBPercent,
# MAGIC   prf.OfferDiscountBAmount,
# MAGIC   prf.OfferDiscountCName,
# MAGIC   prf.OfferDiscountCPercent,
# MAGIC   prf.OfferDiscountCAmount,
# MAGIC   prf.SingleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.SingleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.SingleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.OfferMultiTermDiscountPercent,
# MAGIC   prf.SingleOfferMultiTermDiscountAmount,
# MAGIC   prf.SingleofferNetPriceAfterDiscounts,
# MAGIC   prf.SingleofferTotalDiscountPercent,
# MAGIC   prf.SingleOfferTotalDiscountAmount,
# MAGIC   prf.SingleOfferTaxAmount,
# MAGIC   prf.SingleOfferYouPayPrice,
# MAGIC   prf.SingleOfferNumberOfPayments,
# MAGIC   prf.SingleOfferDownPaymentAmount,
# MAGIC   prf.SingleOfferMonthlyRecurringPayment,
# MAGIC   prf.SingleOfferFrequencyOfPayments,
# MAGIC   prf.BundleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.BundleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.BundleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.BundleOfferMultiTermDiscountAmount,
# MAGIC   prf.BundleofferNetPriceAfterDiscounts,
# MAGIC   prf.BundleofferTotalDiscountPercent,
# MAGIC   prf.BundleOfferTotalDiscountAmount,
# MAGIC   prf.BundleOfferTaxAmount,
# MAGIC   prf.BundleOfferYouPayPrice,
# MAGIC   prf.BundleOfferNumberOfPayments,
# MAGIC   prf.BundleOfferDownPaymentAmount,
# MAGIC   prf.BundleOfferMonthlyRecurringPayment,
# MAGIC   prf.BundleOfferFrequencyOfPayments,
# MAGIC   upper(trim(prf.source)) as source,
# MAGIC   prf.pricingLeadType,
# MAGIC   prf.IsEvergreen,
# MAGIC   prf.ownershipstartedon,
# MAGIC   round((prf.singleofferyoupayprice/prf.numberOfYears),2) as SingleOfferPricePerYear,
# MAGIC   round((prf.singleofferyoupayprice/(prf.numberOfYears*12)),2) as Singleofferpricepermonth,
# MAGIC   round((prf.bundleofferyoupayprice/prf.numberOfYears),2) as Bundleofferpriceperyear,
# MAGIC   round((prf.bundleofferyoupayprice/(prf.numberOfYears*12)),2) as Bundleofferpricepermonth,
# MAGIC   round(((SingleOfferYouPayPrice-SingleofferNetPriceAfterDiscounts)/SingleOfferYouPayPrice *100),2) as Offertaxpercentage,
# MAGIC   cast(prf.SoEgDpTax AS DECIMAL(10, 2)) as SoEgDpTax,
# MAGIC   cast(prf.BoEgDpTax AS DECIMAL(10, 2)) as BoEgDpTax,
# MAGIC   cast(prf.BoEgMonTax AS DECIMAL(10, 2)) as BoEgMonTax,
# MAGIC   cast(prf.SoEgMonTax AS DECIMAL(10, 2)) as SoEgMonTax,
# MAGIC   prf.EgNoOfPmntsDown,
# MAGIC   prf.EgInitCovPeriod
# MAGIC   FROM
# MAGIC   kert1asur.adhoc.RTPricingFieldCalPaymentOption prf 
# MAGIC   where prf.businessunitid in ('CWO','Dealer');

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------
# MAGIC Final table is created with those records for which we have term to offer, base price is caluclated
# MAGIC discounts are available, Tax is calculated and payment option is derrived
# MAGIC ------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTCdmPricingFieldCalculation_GEA 
# MAGIC AS
# MAGIC SELECT distinct
# MAGIC   prf.assetId,
# MAGIC   prf.businessunitid,
# MAGIC   prf.plc,
# MAGIC   prf.BasePrice as consumerBasePrice,
# MAGIC   prf.MarketingProgramId as marketingProgramID,
# MAGIC   prf.numberOfYears as OfferTerm,
# MAGIC   prf.coPayAmount,
# MAGIC   prf.ageinyears,
# MAGIC   false as DateOfPurchaseCoverage,
# MAGIC   false as ExcludeTax,
# MAGIC   prf.programName,
# MAGIC   prf.programTypeCode,
# MAGIC   prf.StartDateDelayPeriod,
# MAGIC   prf.dealerId,
# MAGIC   prf.OfferDiscountAName,
# MAGIC   prf.OfferDiscountAPercent,
# MAGIC   prf.OfferDiscountAAmount,
# MAGIC   prf.OfferDiscountBName,
# MAGIC   prf.OfferDiscountBPercent,
# MAGIC   prf.OfferDiscountBAmount,
# MAGIC   prf.OfferDiscountCName,
# MAGIC   prf.OfferDiscountCPercent,
# MAGIC   prf.OfferDiscountCAmount,
# MAGIC   prf.SingleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.SingleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.SingleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.OfferMultiTermDiscountPercent,
# MAGIC   prf.SingleOfferMultiTermDiscountAmount,
# MAGIC   prf.SingleofferNetPriceAfterDiscounts,
# MAGIC   prf.SingleofferTotalDiscountPercent,
# MAGIC   prf.SingleOfferTotalDiscountAmount,
# MAGIC   prf.SingleOfferTaxAmount,
# MAGIC   prf.SingleOfferYouPayPrice,
# MAGIC   prf.SingleOfferNumberOfPayments,
# MAGIC   prf.SingleOfferDownPaymentAmount,
# MAGIC   prf.SingleOfferMonthlyRecurringPayment,
# MAGIC   prf.SingleOfferFrequencyOfPayments,
# MAGIC   prf.BundleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.BundleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.BundleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.BundleOfferMultiTermDiscountAmount,
# MAGIC   prf.BundleofferNetPriceAfterDiscounts,
# MAGIC   prf.BundleofferTotalDiscountPercent,
# MAGIC   prf.BundleOfferTotalDiscountAmount,
# MAGIC   prf.BundleOfferTaxAmount,
# MAGIC   prf.BundleOfferYouPayPrice,
# MAGIC   prf.BundleOfferNumberOfPayments,
# MAGIC   prf.BundleOfferDownPaymentAmount,
# MAGIC   prf.BundleOfferMonthlyRecurringPayment,
# MAGIC   prf.BundleOfferFrequencyOfPayments,
# MAGIC   upper(trim(prf.source)) as source,
# MAGIC   prf.pricingLeadType,
# MAGIC   prf.IsEvergreen,
# MAGIC   prf.ownershipstartedon,
# MAGIC   round((prf.singleofferyoupayprice/prf.numberOfYears),2) as SingleOfferPricePerYear,
# MAGIC   round((prf.singleofferyoupayprice/(prf.numberOfYears*12)),2) as Singleofferpricepermonth,
# MAGIC   round((prf.bundleofferyoupayprice/prf.numberOfYears),2) as Bundleofferpriceperyear,
# MAGIC   round((prf.bundleofferyoupayprice/(prf.numberOfYears*12)),2) as Bundleofferpricepermonth,
# MAGIC   round(((SingleOfferYouPayPrice-SingleofferNetPriceAfterDiscounts)/SingleOfferYouPayPrice *100),2) as Offertaxpercentage,
# MAGIC   cast(prf.SoEgDpTax AS DECIMAL(10, 2)) as SoEgDpTax,
# MAGIC   cast(prf.BoEgDpTax AS DECIMAL(10, 2)) as BoEgDpTax,
# MAGIC   cast(prf.BoEgMonTax AS DECIMAL(10, 2)) as BoEgMonTax,
# MAGIC   cast(prf.SoEgMonTax AS DECIMAL(10, 2)) as SoEgMonTax,
# MAGIC   prf.EgNoOfPmntsDown,
# MAGIC   prf.EgInitCovPeriod
# MAGIC   FROM
# MAGIC   kert1asur.adhoc.RTPricingFieldCalPaymentOption prf 
# MAGIC   where prf.businessunitid in ('GEA') ;
# MAGIC   -- and prf.pricingleadtype='OOW';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*----------------------------------------------------
# MAGIC Final table is created with those records for which we have term to offer, base price is caluclated
# MAGIC discounts are available, Tax is calculated and payment option is derrived
# MAGIC ------------------------------------------------------*/
# MAGIC create or replace TABLE kert1asur.adhoc.RTCdmPricingFieldCalculation_Lowes 
# MAGIC AS
# MAGIC SELECT distinct
# MAGIC   prf.assetId,
# MAGIC   prf.businessunitid,
# MAGIC   prf.plc,
# MAGIC   prf.BasePrice as consumerBasePrice,
# MAGIC   prf.MarketingProgramId as marketingProgramID,
# MAGIC   prf.numberOfYears as OfferTerm,
# MAGIC   prf.coPayAmount,
# MAGIC   prf.ageinyears,
# MAGIC   false as DateOfPurchaseCoverage,
# MAGIC   false as ExcludeTax,
# MAGIC   prf.programName,
# MAGIC   prf.programTypeCode,
# MAGIC   prf.StartDateDelayPeriod,
# MAGIC   prf.dealerId,
# MAGIC   prf.OfferDiscountAName,
# MAGIC   prf.OfferDiscountAPercent,
# MAGIC   prf.OfferDiscountAAmount,
# MAGIC   prf.OfferDiscountBName,
# MAGIC   prf.OfferDiscountBPercent,
# MAGIC   prf.OfferDiscountBAmount,
# MAGIC   prf.OfferDiscountCName,
# MAGIC   prf.OfferDiscountCPercent,
# MAGIC   prf.OfferDiscountCAmount,
# MAGIC   prf.SingleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.SingleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.SingleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.OfferMultiTermDiscountPercent,
# MAGIC   prf.SingleOfferMultiTermDiscountAmount,
# MAGIC   prf.SingleofferNetPriceAfterDiscounts,
# MAGIC   prf.SingleofferTotalDiscountPercent,
# MAGIC   prf.SingleOfferTotalDiscountAmount,
# MAGIC   prf.SingleOfferTaxAmount,
# MAGIC   prf.SingleOfferYouPayPrice,
# MAGIC   prf.SingleOfferNumberOfPayments,
# MAGIC   prf.SingleOfferDownPaymentAmount,
# MAGIC   prf.SingleOfferMonthlyRecurringPayment,
# MAGIC   prf.SingleOfferFrequencyOfPayments,
# MAGIC   prf.BundleOfferTotalAppliedDiscountPercent,
# MAGIC   prf.BundleOfferTotalAppliedDiscountAmount,
# MAGIC   prf.BundleOfferPriceAfterAppliedDiscounts,
# MAGIC   prf.BundleOfferMultiTermDiscountAmount,
# MAGIC   prf.BundleofferNetPriceAfterDiscounts,
# MAGIC   prf.BundleofferTotalDiscountPercent,
# MAGIC   prf.BundleOfferTotalDiscountAmount,
# MAGIC   prf.BundleOfferTaxAmount,
# MAGIC   prf.BundleOfferYouPayPrice,
# MAGIC   prf.BundleOfferNumberOfPayments,
# MAGIC   prf.BundleOfferDownPaymentAmount,
# MAGIC   prf.BundleOfferMonthlyRecurringPayment,
# MAGIC   prf.BundleOfferFrequencyOfPayments,
# MAGIC   upper(trim(prf.source)) as source,
# MAGIC   prf.pricingLeadType,
# MAGIC   prf.IsEvergreen,
# MAGIC   prf.ownershipstartedon,
# MAGIC   round((prf.singleofferyoupayprice/prf.numberOfYears),2) as SingleOfferPricePerYear,
# MAGIC   round((prf.singleofferyoupayprice/(prf.numberOfYears*12)),2) as Singleofferpricepermonth,
# MAGIC   round((prf.bundleofferyoupayprice/prf.numberOfYears),2) as Bundleofferpriceperyear,
# MAGIC   round((prf.bundleofferyoupayprice/(prf.numberOfYears*12)),2) as Bundleofferpricepermonth,
# MAGIC   round(((SingleOfferYouPayPrice-SingleofferNetPriceAfterDiscounts)/SingleOfferYouPayPrice *100),2) as Offertaxpercentage,
# MAGIC   cast(prf.SoEgDpTax AS DECIMAL(10, 2)) as SoEgDpTax,
# MAGIC   cast(prf.BoEgDpTax AS DECIMAL(10, 2)) as BoEgDpTax,
# MAGIC   cast(prf.BoEgMonTax AS DECIMAL(10, 2)) as BoEgMonTax,
# MAGIC   cast(prf.SoEgMonTax AS DECIMAL(10, 2)) as SoEgMonTax,
# MAGIC   prf.EgNoOfPmntsDown,
# MAGIC   prf.EgInitCovPeriod
# MAGIC   FROM
# MAGIC   kert1asur.adhoc.RTPricingFieldCalPaymentOption prf 
# MAGIC   where prf.businessunitid in ('Lowes');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kert1asur.adhoc.RTCdmPricingFieldCalculation as
# MAGIC select
# MAGIC   md5(assetid||source||offerterm||nvl(singleoffernumberofpayments,'NA')||SingleOfferTaxAmount||nvl(bundleoffernumberofpayments,'NA')||BundleOfferTaxAmount) as RTPID,
# MAGIC   *
# MAGIC from
# MAGIC   kert1asur.adhoc.RTCdmPricingFieldCalculation_GEA 
# MAGIC union all
# MAGIC select
# MAGIC md5(assetid||source||offerterm||nvl(singleoffernumberofpayments,'NA')||SingleOfferTaxAmount||nvl(bundleoffernumberofpayments,'NA')||BundleOfferTaxAmount) as RTPID,
# MAGIC   *
# MAGIC from
# MAGIC   kert1asur.adhoc.RTCdmPricingFieldCalculation_CWO_DEALER
# MAGIC union all
# MAGIC select
# MAGIC md5(assetid||source||offerterm||nvl(singleoffernumberofpayments,'NA')||SingleOfferTaxAmount||nvl(bundleoffernumberofpayments,'NA')||BundleOfferTaxAmount) as RTPID,
# MAGIC   *
# MAGIC from
# MAGIC   kert1asur.adhoc.RTCdmPricingFieldCalculation_Lowes;