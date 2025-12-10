--DROP TABLE twm_RESULT.MA_Corporate_Groups_Base;
CREATE MULTISET TABLE twm_RESULT.HW_Corporate_Groups_Base AS (

SEL
DSI.Subscription_Id
, RP.Rate_Plan_Group
, RP.Rate_Plan_Category
, CBSF.XNET_Base_Segmentation
, CASE 
    WHEN ZeroIfNull(Total_Recharge) = 0 THEN '0'
    WHEN ZeroIfNull(Total_Recharge) BETWEEN 1 AND 10 THEN '1--10'
    WHEN ZeroIfNull(Total_Recharge) BETWEEN 10 AND 30 THEN '10--30'
    WHEN ZeroIfNull(Total_Recharge) BETWEEN 30 AND 50 THEN '30--50'
    ELSE '50++' END AS Total_Recharge_Tier
, CASE 
    WHEN RP.Rate_Plan_Category = 'PostPaid' THEN ZeroIfNull(CRMP.Decile)
    WHEN RP.Rate_Plan_Category = 'PrePaid' THEN ZeroIfNull(CPRP.Decile)
    ELSE 0 END AS Corporate_Decile
FROM VDB.DETAILED_SUBSCRIPTION_INFO AS DSI
LEFT JOIN VDB.rate_plan AS RP ON RP.Rate_Plan_Product_Id = DSI.Rate_Plan_Product_Id
LEFT JOIN VDB.Corp_Base_Segmentation_Final AS CBSF ON CBSF.Subscription_Id = DSI.Subscription_Id
LEFT JOIN (

SEL
CPRP.Subscription_Id
, CPRP.Decile
FROM VDB.Corp_Pre_Recharge_Propensity AS CPRP
WHERE CPRP.Running_date = DATE - 1

) AS CPRP ON CPRP.Subscription_Id = DSI.Subscription_Id
LEFT JOIN (

SEL
CRMP.Subscription_Id
, CRMP.Decile
FROM VDB.Corporate_Recharge_Model_Propensity AS CRMP
WHERE CRMP.Running_date = DATE - 1

) AS CRMP ON CRMP.Subscription_Id = DSI.Subscription_Id
LEFT JOIN (

SEL
CRD.Subscription_Id
, Sum(CRD.Recharge_amt) AS Total_Recharge
FROM VDB.CVM_Recharge_Daily AS CRD
WHERE 1 = 1
AND CRD.Payment_Date BETWEEN DATE '2024-11-01' AND DATE '2024-11-30'
GROUP BY 1

) AS RCH ON RCH.Subscription_Id = DSI.Subscription_Id
WHERE 1 = 1
AND DSI.Subscription_Status_Group_Cd NOT IN (-1, 4)
AND RP.Rate_Plan_Group = 'Business'
GROUP BY 1, 2, 3, 4, 5, 6

) WITH DATA PRIMARY INDEX (subscription_id);

SEL * FROM twm_RESULT.HW_Corporate_Groups_Base;

--***************************************************
--============================================
CREATE MULTISET TABLE twm_result.HW_RCH_Corp AS (

WITH base AS (

SEL
bs.Subscription_Id
, bs.Rate_Plan_Group
, bs.Rate_Plan_Category
, CASE WHEN bs.XNET_Base_Segmentation = 1 THEN 'Seg_1'
        WHEN bs.XNET_Base_Segmentation = 2 THEN 'Seg_2'
        WHEN bs.XNET_Base_Segmentation = 3 THEN 'Seg_3'
        END AS Base_Segmentation
, bs.Total_Recharge_Tier
, CASE WHEN ((bs.Rate_Plan_Category = 'PostPaid' AND bs.Corporate_Decile IN (NULL, 1, 2, 3, 4, 5)) OR (bs.Rate_Plan_Category = 'PrePaid' AND bs.Corporate_Decile IN (NULL, 1, 2))) THEN 'High'
       WHEN ((bs.Rate_Plan_Category = 'PostPaid' AND bs.Corporate_Decile IN (0, 6, 7, 8, 9, 10)) OR (bs.Rate_Plan_Category = 'PrePaid' AND bs.Corporate_Decile IN (0, 3, 4, 5, 6, 7, 8, 9, 10))) THEN 'Low'
       END AS Decile_Tier
FROM twm_result.HW_Corporate_Groups_Base AS bs
WHERE 1 = 1
AND bs.XNET_Base_Segmentation IN  (1,2,3)
AND ((bs.Rate_Plan_Category = 'PostPaid' AND bs.Corporate_Decile IN (NULL, 1, 2, 3, 4, 5,0, 6, 7, 8, 9, 10)) 
OR (bs.Rate_Plan_Category = 'PrePaid' AND bs.Corporate_Decile IN (NULL, 1, 2,0, 3, 4, 5, 6, 7, 8, 9, 10)))
GROUP BY 1, 2, 3, 4, 5, 6

)

SEL
bs.Subscription_Id
, bs.Total_Recharge_Tier
, base_segmentation
, Decile_Tier
, Coalesce (CECM.Rech_Plat_Redem_Flag,'N') Rech_Plat_Redem_Flag
, ZeroIfNull(Sum(CASE WHEN ASRM.Revenue_Date BETWEEN DATE '2024-09-01' + 1 AND DATE '2024-09-30' + 1 THEN ASRM.Total_Revenue - ASRM.Bill_Amount END)) AS M3_Total_Rev
, ZeroIfNull(Sum(CASE WHEN ASRM.Revenue_Date BETWEEN DATE '2024-10-01' + 1 AND DATE '2024-10-31' + 1 THEN ASRM.Total_Revenue - ASRM.Bill_Amount END)) AS M2_Total_Rev
, ZeroIfNull(Sum(CASE WHEN ASRM.Revenue_Date BETWEEN DATE '2024-11-01' + 1 AND DATE '2024-11-30' + 1 THEN ASRM.Total_Revenue - ASRM.Bill_Amount END)) AS M1_Total_Rev
, CASE WHEN _AB.Subscription_Id IS NULL AND MCG.UCG_Flag IN (5,7) THEN 1 ELSE 0 END AS should_be_selected --updated to 5,7 instead of 6,8
FROM base AS bs
LEFT JOIN VDB.AGG_SUBS_REVENUE_MONTHLY AS ASRM ON ASRM.subscription_id =bs.subscription_id AND ASRM.Revenue_Date BETWEEN DATE '2024-09-01' + 1 AND DATE '2024-11-30' + 1
LEFT JOIN (

SEL 
CECM.Subscription_id
, CECM.Rech_Plat_Redem_Flag
FROM VDB.cvm_engagement_Corporate_monthly AS CECM
WHERE 1 = 1
AND CECM.Cal_Month = 11
AND CECM.Cal_Year = 2024
GROUP BY 1, 2

) AS CECM ON CECM.Subscription_id = bs.subscription_id
LEFT JOIN VDB.AB_Testing_CG_TG AS _AB ON _AB.Subscription_ID = bs.Subscription_Id
LEFT JOIN VDB.Master_Control_Group AS _MCG ON _MCG.Subscription_Id = bs.Subscription_Id
GROUP BY 1, 2, 3, 4,5,9

) WITH DATA PRIMARY INDEX (Subscription_Id);

SEL * FROM twm_result.HW_RCH_Corp;

---=====================================================
--SIZING
SEL
'before excluding' base
,Count(Subscription_Id) AS No_of_Subs
, Sum(M1_Total_Rev) AS M1_Total_Rev
, Sum(M2_Total_Rev) AS M2_Total_Rev
, Sum(M3_Total_Rev) AS M3_Total_Rev
FROM twm_result.HW_RCH_Corp
UNION ALL
SEL
'after excluding' base
,Count(Subscription_Id) AS No_of_Subs
, Sum(M1_Total_Rev) AS M1_Total_Rev
, Sum(M2_Total_Rev) AS M2_Total_Rev
, Sum(M3_Total_Rev) AS M3_Total_Rev
FROM twm_result.HW_RCH_Corp
WHERE should_be_selected = 1;


SEL * FROM twm_result.HW_RCH_Corp

--======================================================
SEL * FROM twm_result.HW_RCH_Corp_groups;
SEL * FROM twm_result.HW_RCH_Corp_groups_pre_validation_table;
SEL * FROM twm_result.HW_RCH_Corp_groups_post_validation_table;

---=====================================================

-- Release the old Subs in the Archive model
/*
INSERT INTO anproddb.AB_Testing_CG_TG_archive
SEL 
ABT.Subscription_ID
, ABT.Tst_ControlGroup_ID
, ABT.Tst_TargetGroup_ID
, Current_Date AS Termination_date
FROM VDB.AB_Testing_CG_TG AS ABT
WHERE 1 = 1
AND (ABT.Tst_TargetGroup_ID IN (4215, 4216, 4217, 4218, 4219)
OR ABT.Tst_ControlGroup_ID IN (4224, 4225, 4226, 4228, 4227));

--==============================================================================
-- Delete the old Subs

DEL FROM anproddb.AB_Testing_CG_TG AS ABT
WHERE 1 = 1
AND (ABT.Tst_TargetGroup_ID IN (4215, 4216, 4217, 4218, 4219)
OR ABT.Tst_ControlGroup_ID IN (4224, 4225, 4226, 4228, 4227));*/

--================================================================================
-- Insert new Subs

INSERT INTO anproddb.AB_Testing_CG_TG 
SEL ab.Subscription_ID
,0 Tst_ControlGroup_ID
,4257 Tst_TargetGroup_ID
FROM twm_result.HW_RCH_Corp_groups ab
WHERE GrouP_Name = 0
AND ab.Subscription_ID NOT IN (SEL Subscription_ID FROM VDB.AB_Testing_CG_TG);


INSERT INTO anproddb.AB_Testing_CG_TG 
SEL ab.Subscription_ID
,4256 Tst_ControlGroup_ID
,0 Tst_TargetGroup_ID
FROM twm_result.HW_RCH_Corp_groups ab
WHERE GrouP_Name = 1
AND ab.Subscription_ID NOT IN (SEL Subscription_ID FROM VDB.AB_Testing_CG_TG);

--==========================================================================================
/*-- Update Old Metadata

UPDATE anproddb.AB_Testing_CG_TG_metadata
SET
Archived_Date = Current_Date
, Archived_Flag = 'Y'

WHERE 1 = 1
AND Developer_Name = 'Mostafa Ahmed'
AND Group_id IN (4215, 4216, 4217, 4218, 4219, 4224, 4225, 4226, 4228, 4227)
AND Insertion_Date = DATE '2022-09-01';*/

--==================================================================================================================
-- Insert New Metadata
INSERT INTO anproddb.AB_Testing_CG_TG_metadata (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);


