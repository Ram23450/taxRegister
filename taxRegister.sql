CREATE or alter	PROCEDURE FIN.TAX_SP_TXR_Register_Report_Insert
(        
 @FromDate DATE,  
 @ToDate  DATE  
)         
AS   
/*  

 EXEC       FIN.TAX_SP_TXR_Register_Report_Insert  
 @FromDate = '01-Sep-2021', @ToDate = '30-Sep-2021'  

*/  
BEGIN        
 SET NOCOUNT ON;    
 
 DECLARE @TaxApplicability VARCHAR(250), @CompTaxApplicability VARCHAR(250);

 SET		@CompTaxApplicability = (SELECT TOP 1 MDTV_Comparable_Validation_Field 
							FROM	EDWSTG40.COM.GEN_M_DT_Validations
							WHERE	MDTV_DT_Code = 30179 AND MDTV_DOPT_Code='V' 
									AND MDTV_Validation_Field = 'TaxRegApplicability');

DROP TABLE IF EXISTS #TempCompApplicabilityMaster;
CREATE TABLE #TempCompApplicabilityMaster(strCompApplicable VARCHAR(250));

INSERT	INTO #TempCompApplicabilityMaster(strCompApplicable)
SELECT	VALUE FROM string_split(@CompTaxApplicability,'~');
      
 --local variable declaration  
 DECLARE @lFromDate DATE = @FromDate,  
   @lToDate DATE = @ToDate;   
  
   DELETE FROM FIN.TAX_T_Register_Report   
   WHERE TRR_SourceDocumentDate >= @lFromDate     
     AND TRR_SourceDocumentDate <= @lToDate;  
      
 --Temp table creation  
 CREATE TABLE #temp_Register(CompanyCode INT, BRType INT, AccountingPeriod INT,   
   SourceDocumentNumber VARCHAR(30), SourceDocumentDate DATE,   
   InvoiceNo VARCHAR(50), InvoiceDate VARCHAR(30), InvoiceType VARCHAR(15),       
   OrderNo VARCHAR(30), OrderDate VARCHAR(30), OrderType INT, OrderTypeDesc VARCHAR(50),  
   ACCode VARCHAR(15), ACDesc VARCHAR(300), JobCode VARCHAR(300), JobDesc VARCHAR(300),  
   GSTIN VARCHAR(30), BAGSTIN VARCHAR(30), BACode VARCHAR(15), BAName VARCHAR(300),       
   CurrencyCode INT, CurrencyDesc VARCHAR(100), ItemSerialNo INT,   
   ItemCode Varchar(50), ItemDesc Varchar(MAX), UOMCode INT, UOMDesc VARCHAR(100),   
   Qty MONEY Default 0, BasicRate MONEY Default 0, TaxableAmt MONEY DEFAULT 0,   
   TaxAmt MONEY DEFAULT 0, TotalAmt MONEY DEFAULT 0,        
   IGSTRate MONEY DEFAULT 0, IGSTAmt MONEY DEFAULT 0,   
   CGSTRate MONEY DEFAULT 0, CGSTAmt MONEY DEFAULT 0,   
   SGSTRate MONEY DEFAULT 0, SGSTAmt MONEY DEFAULT 0,   
   UTGSTRate MONEY DEFAULT 0, UTGSTAmt MONEY DEFAULT 0,        
   RIGSTRate MONEY DEFAULT 0, RIGSTAmt MONEY DEFAULT 0,  
   RCGSTRate MONEY DEFAULT 0, RCGSTAmt MONEY DEFAULT 0,   
   RSGSTRate MONEY DEFAULT 0, RSGSTAmt MONEY DEFAULT 0,   
   RUTGSTRate MONEY DEFAULT 0, RUTGSTAmt MONEY DEFAULT 0,    
   CCessRate MONEY DEFAULT 0, CCessAmt MONEY DEFAULT 0,          
   VGCCRate MONEY DEFAULT 0,VGCCAmt MONEY DEFAULT 0,   
   RCMGCCRate MONEY DEFAULT 0, RCMGCCAmt MONEY DEFAULT 0,    
   DiscountRate MONEY DEFAULT 0, DiscountAmt MONEY DEFAULT 0,  
   PackingForwardingRate MONEY DEFAULT 0, PackingForwardingAmt MONEY DEFAULT 0,  
   InsuranceRate MONEY DEFAULT 0, InsuranceAmt MONEY DEFAULT 0,  
   FreightChargesRate MONEY DEFAULT 0, FreightChargesAmt MONEY DEFAULT 0,  
   OthersRate MONEY DEFAULT 0, OthersAmt MONEY DEFAULT 0,  
   TCSRate MONEY DEFAULT 0, TCSAmt MONEY DEFAULT 0,  
   AdvAdjIGSTRate MONEY DEFAULT 0, AdvAdjIGSTAmt MONEY DEFAULT 0,  
   AdvAdjCGSTRate MONEY DEFAULT 0, AdvAdjCGSTAmt MONEY DEFAULT 0,   
   AdvAdjSGSTRate MONEY DEFAULT 0, AdvAdjSGSTAmt MONEY DEFAULT 0,   
   AdvAdjUTGSTRate MONEY DEFAULT 0, AdvAdjUTGSTAmt MONEY DEFAULT 0,        
   ITC CHAR(1), RCM CHAR(1),   
   FromCountryCode INT, FromCountryName VARCHAR(150),  
   ToCountryCode INT, ToCountryName VARCHAR(150),  
   SrcStateCode INT, SrcStateName VARCHAR(150),  
   DelStateCode INT, DelStateName VARCHAR(150),   
   GorS CHAR(1),   
   HSNSACCode VARCHAR(15), HSNSACDesc VARCHAR(MAX),   
   ServiceCode VARCHAR(15), ServiceDesc VARCHAR(MAX),   
   BOLNo VARCHAR(50), BOLDate VARCHAR(30), PortCode INT, BOENo VARCHAR(100),        
   JVNo VARCHAR(30), VoucherNo INT, VoucherDate VARCHAR(30),   
   RecoDate VARCHAR(30), Reco2A CHAR(1), --2A-ReconStatus        
   BRSLNO INT, BRNo VARCHAR(30), BRDate VARCHAR(30),   
   MRNNo VARCHAR(30), MRNDate VARCHAR(30), WOBillNo VARCHAR(30), WOBillDate VARCHAR(30),   
   IRNNumber VARCHAR(100), AcknowledgeNumber VARCHAR(50), AcknowledgeDate VARCHAR(30),  
   EwayBillNumber VARCHAR(50), EwayBillDate VARCHAR(30),  
   RefDTCode INT, SupplyType VARCHAR(10),   
   ASPExprtd CHAR(1), DocType VARCHAR(2), DocStatus VARCHAR(10),        
   StrICCode  VARCHAR(200), StrRegionCode  VARCHAR(200), StrClusterCode VARCHAR(200),  
   DebitJobCode VARCHAR(300),   
   invoiceGroup VARCHAR(15), InternalInvNo VARCHAR(30),  
   LR_Number VARCHAR(30), GSTReleaseMode VARCHAR(250),  
   GSTR2BITCStatus VARCHAR(50), GSTR2B_Month DATE, FJV_Date DATE,  
   FJV_Number VARCHAR(30), JV_No VARCHAR(30),  
   PJV_SJV_Date DATE, -- included by Priya on Mar 2022  
   CompanyGSTINAsPerEBR VARCHAR(30), BAGSTINAsPerEBR VARCHAR(30),  
   ActualInvoiceType VARCHAR(15) , Certification_date date  , TaxpayerCategory varchar(200));       
  
 CREATE TABLE #Tmp_Columns (TMP_Column VARCHAR(15));     
   
 INSERT INTO #temp_Register(CompanyCode, BRType, SourceDocumentNumber, SourceDocumentDate, ACCode,         
   InvoiceNo, InvoiceDate, OrderNo, AccountingPeriod, GSTIN,         
   BAGSTIN, BACode, JobCode, CurrencyCode, ItemSerialNo, ItemCode,         
   UOMCode, Qty, BasicRate, TaxableAmt, TaxAmt, TotalAmt,        
   OrderType,FromCountryCode, SrcStateCode, ToCountryCode, DelStateCode,        
   GorS, HSNSACCode, BOLNo, BOLDate, PortCode, JVNo, VoucherNo, VoucherDate,         
   Reco2A, RecoDate,BRNo, BRDate, MRNNo, MRNDate, WOBillNo, WOBillDate,   
   InvoiceType, RefDTCode, DocType, ITC, RCM, BOENo, ServiceCode, ServiceDesc,  
   IRNNumber, AcknowledgeNumber, AcknowledgeDate, DebitJobCode,  
   LR_Number, GSTReleaseMode)        
 SELECT H.HGR_Company_Code, H.HGR_BR_Type_Code, H.HGR_Register_Number, H.HGR_Register_Date, --FORMAT(H.HGR_Register_Date, 'dd-MMM-yyyy'),   
 H.HGR_AC_Code,  
   D.DGR_Invoice_Number, FORMAT(D.DGR_Invoice_Date, 'dd-MMM-yyyy'), H.HGR_Order_Number,   
   H.HGR_Accounting_Period, H.HGR_GSTIN_Number,         
   H.HGR_BA_GSTIN_Number,H.HGR_BA_Code, H.HGR_Job_Code, H.HGR_Currency_Code,d.DGR_Serial_Number, D.DGR_ITEM_Code,         
   D.DGR_UOM_Code,d.DGR_Qty, d.DGR_Basic_Rate,  d.DGR_Amount, d.DGR_Tax_Amount,D.DGR_Total_Amount,        
   iif(G.DGRG_Order_Details_Code=0, G.DGRG_Order_Type_Code, G.DGRG_Order_Details_Code) DGRG_Order_Details_Code,        
   d.DGR_From_Country_Code, D.DGR_From_State_Code,d.DGR_To_Country_Code, D.DGR_To_State_Code,        
   D.DGR_GorS_Tag, D.DGR_HSN_SAC_Code, DGRG_BOE_Number, FORMAT(DGRG_BOE_Date, 'dd-MMM-yyyy'),  DGRG_PORT_Code,        
   H.HGR_JV_Reference_Number,H.HGR_Voucher_Number, FORMAT(H.HGR_Voucher_Date, 'dd-MMM-yyyy'),         
   IIF(D.DGR_Reonciliation_Code IS NULL, 'N', 'Y'), FORMAT(d.DGR_Reconciliation_Date, 'dd-MMM-yyyy'),   
   g.DGRG_BR_Number, FORMAT(g.DGRG_BR_Date, 'dd-MMM-yyyy'), g.DGRG_MRN_Number, FORMAT(g.DGRG_MRN_Date, 'dd-MMM-yyyy'),   
   G.DGRG_WO_Bill_Number, FORMAT(G.DGRG_WO_Bill_Date, 'dd-MMM-yyyy'), G.DGRG_Invoice_Type, H.HGR_Reference_DT_Code,DGR_Document_Type,        
   DGR_ITC_Applicable, DGR_RCM_Applicable, G.DGRG_BOE_Number, D.DGR_Service_Code, TN.mtn_description,  
   DGRG_IRN_Number, DGRG_Acknowledgement_Number, FORMAT(DGRG_Acknowledgement_Date, 'dd-MMM-yyyy')  ,  
   DGR_Debit_Job_Code, HGR_LR_Number, HGR_GST_Release_Mode -- included by Priya on Mar 2022      
 FROM EDWSTG40.TAX.FAS_H_GST_Register H WITH (NOLOCK)         
   INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register D WITH (NOLOCK)  
    ON H.HGR_Register_Number = D.DGR_Register_Number    
   INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_General G WITH (NOLOCK)  
    ON H.HGR_Register_Number = G.DGRG_Register_Number     
   LEFT JOIN EDWSTG40.MDM.STM_M_Tax_Nature TN WITH (NOLOCK)  
    ON TN.MTN_Country_Code = D.DGR_To_Country_Code AND TN.MTN_Code = D.DGR_Service_Code   
    AND TN.MTN_Tax_Type=101  
 WHERE H.HGR_Register_Date >= @lFromDate     
   AND H.HGR_Register_Date <= @lToDate  
   AND H.HGR_Register_Number NOT like '%FGJ%';    
      
	 ---Modified by Guru For capturing Taxpayer category 

Update #temp_Register
 SET  TaxpayerCategory = MGST_Taxpayer_Type
 from edwstg40.MDM.BAM_M_GSTIN 
 where BAGSTIN = MGST_GST_Number and BACode = MGST_BA_Code and CompanyCode = MGST_Company_Code


 UPDATE tmp        
 SET  tmp.ItemDesc = isnull(BOQ_Description, '')  
 FROM #temp_Register tmp   
   INNER JOIN EDWSTG40.cim.BOQ_T_Client BOQ  WITH (NOLOCK)  
    ON Tmp.JobCode = BOQ_Job_Code        
     AND tmp.InvoiceType = BOQ_Invoice_Type        
     AND tmp.BACode = BOQ_Customer_Code        
     AND tmp.OrderNo = BOQ_Order_No        
     AND Tmp.CurrencyCode = BOQ_Currency_Code        
     --AND TRIM(SUBSTRING(Tmp.ItemCode, 1,CHARINDEX('-', Tmp.ItemCode) - 1))  = TRIM(BOQ_Client_BOQ)  
     AND TRIM(SUBSTRING(Tmp.ItemCode, 1 ,  
      case when  CHARINDEX('-', Tmp.ItemCode ) = 0 then LEN(Tmp.ItemCode)   
      else CHARINDEX('-', Tmp.ItemCode) - 1 end)) = TRIM(BOQ_Client_BOQ)  
     AND tmp.BRType = 1;  
  
 UPDATE tmp     
 SET  tmp.ItemDesc = b.BOQ_Description    
 FROM #temp_Register tmp, EDWSTG40.CIM.BOQ_T_Client b   
   INNER JOIN EDWSTG40.CIM.BOQ_T_Tender c   
    ON TBOQ_Job_Code = BOQ_Job_Code   
    AND TBOQ_Invoice_Type = BOQ_Invoice_Type  
    AND TBOQ_Customer_Code = BOQ_Customer_Code  
    AND TBOQ_Order_No = BOQ_Order_No  
    AND TBOQ_Client_BOQ_Code = BOQ_Client_BOQ  
 WHERE Tmp.JobCode = b.BOQ_Job_Code   
   --AND TRIM(SUBSTRING(Tmp.ItemCode, 1,CHARINDEX('-', Tmp.ItemCode) - 1))  = TRIM(c.TBOQ_Tender_Code)   
   AND TRIM(SUBSTRING(Tmp.ItemCode, 1 ,  
      case when  CHARINDEX('-', Tmp.ItemCode ) = 0 then LEN(Tmp.ItemCode)   
      else CHARINDEX('-', Tmp.ItemCode) - 1 end)) = TRIM(c.TBOQ_Tender_Code)   
   AND tmp.BACode = b.BOQ_Customer_Code AND tmp.OrderNo = b.BOQ_Order_No  
   AND tmp.BRType = 1;  
  
 UPDATE tmp        
 SET  tmp.ItemDesc = MIR_Other_Item_Description  
 FROM #temp_Register tmp   
   INNER JOIN EDWSTG40.CIM.MAS_M_Recovery WITH (NOLOCK)  
    ON Tmp.ItemCode  = MIR_Other_Item_Code + '-Adv'  
     AND tmp.BRType = 1;  
  
 UPDATE tmp        
 SET  tmp.ItemDesc = MIR_Other_Item_Description  
 FROM #temp_Register tmp   
   INNER JOIN EDWSTG40.CIM.MAS_M_Recovery WITH (NOLOCK)  
    ON Tmp.ItemCode  = MIR_Other_Item_Code + '-ded'  
     AND tmp.BRType = 1;  
  
 UPDATE tmp        
    SET    tmp.ItemDesc = isnull(BOQ_Description, '')  
    FROM   #temp_Register tmp   
           INNER JOIN EDWSTG40.cim.BOQ_T_Client BOQ  WITH (NOLOCK)  
           ON     Tmp.JobCode = BOQ_Job_Code        
                  AND tmp.InvoiceType = BOQ_Invoice_Type        
                  AND tmp.BACode = BOQ_Customer_Code        
                  AND tmp.OrderNo = BOQ_Order_No        
                  AND Tmp.CurrencyCode = BOQ_Currency_Code        
                  --AND TRIM((SUBSTRING(Tmp.ItemCode,(0),LEN(Tmp.ItemCode)-(5))))  = TRIM(BOQ_Client_BOQ)  
      AND TRIM(SUBSTRING(Tmp.ItemCode, 1 ,  
       case when  CHARINDEX('-', Tmp.ItemCode ) = 0 then LEN(Tmp.ItemCode)   
       else CHARINDEX('-', Tmp.ItemCode) - 1 end)) = TRIM(BOQ_Client_BOQ)  
                  AND tmp.BRType = 1 AND tmp.ItemDesc IS NULL;  
  
 UPDATE tmp        
 SET  tmp.IRNNumber = IDE_Doc_No,  
   tmp.AcknowledgeNumber = IDE_Ack_No,  
   tmp.AcknowledgeDate = FORMAT(IDE_Ack_Date, 'dd-MMM-yyyy')  
 FROM #temp_Register tmp   
   INNER JOIN EDWSTG40.CIM.INV_D_EINVOICE WITH (NOLOCK)  
    ON Tmp.JobCode = IDE_Job_Code        
     AND tmp.InvoiceType = IDE_Invoice_Type  
     AND tmp.InvoiceNo = IDE_Invoice_No  
     AND IDE_Doc_Type = 'IRN'     
     AND tmp.IRNNumber IS NULL  
     AND tmp.BRType = 1;  
  
 UPDATE tmp        
 SET  tmp.EwayBillNumber = IDE_EWayBill_No,  
   tmp.EwayBillDate = FORMAT(IDE_EWayBill_Date, 'dd-MMM-yyyy')  
 FROM #temp_Register tmp   
   INNER JOIN EDWSTG40.CIM.INV_D_EINVOICE WITH (NOLOCK)  
    ON tmp.JobCode = IDE_Job_Code        
     AND tmp.InvoiceType = IDE_Invoice_Type  
     AND tmp.InvoiceNo = IDE_Invoice_No  
     AND IDE_Doc_Type = 'IRN'  
     AND tmp.BRType = 1;   
   
 UPDATE tmp        
 SET  tmp.ItemDesc = isnull(MMAT_Material_Description, '')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.MDM.GEM_M_Materials mtrl WITH (NOLOCK) on mtrl.MMAT_Material_Code = tmp.ItemCode        
    AND mtrl.MMAT_Company_Code = tmp.CompanyCode AND tmp.BRType = 2;    
   
 UPDATE tmp        
 SET  tmp.OrderDate = FORMAT(HPO_PO_Date, 'dd-MMM-yyyy')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.PRC.ORD_H_Purchase_Orders WITH (NOLOCK)  
    ON HPO_PO_Number = tmp.OrderNo AND tmp.BRType = 2;  
  
 UPDATE tmp        
 SET  tmp.OrderTypeDesc = isnull(CMTD_Trans_Type_Desc, '')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.scmcom.CFM_M_Transaction_Details WITH (NOLOCK)  
    ON CMTD_Trans_Type_Code = 900 AND CMTD_Trans_Detail_Code= tmp.OrderType  
     AND tmp.BRType = 2;  
   
 UPDATE tmp        
 SET  tmp.ItemDesc = isnull(MWI_Description,'')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.MDM.GEM_M_Work_Items_Company itm WITH (NOLOCK)  
    ON MWI_Work_Item_Code = tmp.ItemCode AND tmp.BRType = 3  
    AND CompanyCode = MWI_Company_Code;  
  
 UPDATE tmp        
 SET  tmp.OrderDate = FORMAT(HWO_WO_Date, 'dd-MMM-yyyy'), tmp.OrderType=HWO_WOT_Code  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.WOM.ORD_H_Work_Orders WITH (NOLOCK)  
    ON HWO_WO_Number = tmp.OrderNo AND tmp.BRType = 3;  
  
 UPDATE tmp        
 SET  tmp.OrderTypeDesc = isnull(MWOTP_Description, '')  
 FROM #temp_Register tmp    
   INNER JOIN EDWSTG40.com.CFM_M_WO_Types WITH (NOLOCK) on MWOTP_WOT_Code = tmp.OrderType  
    AND tmp.BRType = 3  
  
 UPDATE tmp        
 SET  tmp.ItemDesc = isnull(MMAT_Material_Description,'')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.MDM.GEM_M_Materials mtrl  WITH (NOLOCK)  
    ON  mtrl.MMAT_Material_Code = tmp.ItemCode        
     AND mtrl.MMAT_Company_Code = tmp.CompanyCode  
     AND tmp.BRType = 7;    
   
 UPDATE tmp        
 SET  tmp.OrderDate = FORMAT(HPO_PO_Date, 'dd-MMM-yyyy')  
 FROM #temp_Register tmp        
   INNER JOIN EDWSTG40.PRC.ORD_H_Purchase_Orders WITH (NOLOCK)  
    ON HPO_PO_Number = tmp.OrderNo AND tmp.BRType = 7;  
  
 UPDATE tmp        
 SET  tmp.OrderTypeDesc = isnull(CMTD_Trans_Type_Desc, '')  
 FROM #temp_Register tmp        
   INNER JOIN  EDWSTG40.scmcom.CFM_M_Transaction_Details WITH (NOLOCK)  
    ON CMTD_Trans_Type_Code = 900 AND CMTD_Trans_Detail_Code = tmp.OrderType  
     AND tmp.BRType = 7;  
  
 --below line added by Neels on 26th Apr 2022 to delete retention COA codes from cash&Bank  
 DELETE FROM #temp_Register  
 WHERE ltrim(rtrim(ItemCode)) in ('00011075','00011046','00011088','00011045','00011090')  
   AND BRType = 3;  
  
---- below lines added by Priya on Mar 2022  
        UPDATE tmp        
        SET  tmp.ItemDesc = isnull(MCOA_Description, '')  
  FROM #temp_Register tmp        
    INNER JOIN        
        EDWSTG40.COM.GEN_M_Charter_Of_Accounts coa on coa.mcoa_coa_code = tmp.ItemCode        
        AND coa.MCOA_Company_Code = tmp.CompanyCode AND tmp.BRType = 3;    
  
 --UPDATE tmp        
 --SET  tmp.BAName = isnull(H.strBAName, '')  
 -- FROM #temp_Register tmp        
 --INNER JOIN         
 -- FIN.FAS_T_Register_Summary_Stage H on H.StrSrcDocNo = tmp.SourceDocumentNumber   
 -- and tmp.BAName is null   AND tmp.BRType = 3;  
   
 UPDATE t        
 SET  IGSTRate = isnull(DGTB_Tax_Rate, 0),    
   IGSTAmt = isnull(DGTB_Tax_Amount, 0),  
   AdvAdjIGSTRate = isnull(DGTB_Tax_Rate, 0),    
   AdvAdjIGSTAmt = isnull(DGTB_Tax_Adjusted_Amount, 0)    
 FROM #temp_Register t    
   INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
    ON t.SourceDocumentNumber = b.DGTB_Register_Number        
    AND t.ItemSerialNo = b.DGTB_Serial_Number        
    AND t.ItemCode = b.DGTB_Item_Code     
    AND b.DGTB_Tax_Code = 'ID1001'   
   INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
    ON  b.DGTB_Register_Number = d.DGR_Register_Number        
    AND b.DGTB_Serial_Number   = d.DGR_Serial_Number;    
   
UPDATE t        
SET  CGSTRate = isnull(DGTB_Tax_Rate , 0),     
  CGSTAmt = isnull(DGTB_Tax_Amount , 0),    
  AdvAdjCGSTRate = isnull(DGTB_Tax_Rate , 0),     
  AdvAdjCGSTAmt = isnull(DGTB_Tax_Adjusted_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)    
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and DGTB_Tax_Code = 'ID1002'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET SGSTRate  = isnull(DGTB_Tax_Rate, 0),     
SGSTAmt  = isnull(DGTB_Tax_Amount, 0),  
AdvAdjSGSTRate  = isnull(DGTB_Tax_Rate, 0),     
AdvAdjSGSTAmt  = isnull(DGTB_Tax_Adjusted_Amount, 0)   
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1003'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET UTGSTRate = isnull(DGTB_Tax_Rate , 0),     
UTGSTAmt = isnull(DGTB_Tax_Amount , 0),  
AdvAdjUTGSTRate = isnull(DGTB_Tax_Rate , 0),     
AdvAdjUTGSTAmt = isnull(DGTB_Tax_Adjusted_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b   WITH (NOLOCK)    
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1004'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET RIGSTRate = isnull(DGTB_Tax_Rate , 0),     
RIGSTAmt = isnull(DGTB_Tax_Amount , 0)   
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code    
and b.DGTB_Tax_Code ='ID1005'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET RCGSTRate = isnull(DGTB_Tax_Rate, 0),     
RCGSTAmt = isnull(DGTB_Tax_Amount , 0)   
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b  WITH (NOLOCK)     
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1006'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET RSGSTRate = isnull(DGTB_Tax_Rate , 0),     
RSGSTAmt = isnull(DGTB_Tax_Amount, 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b  WITH (NOLOCK)     
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1007'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET RUTGSTRate = isnull(DGTB_Tax_Rate , 0),     
RUTGSTAmt = isnull(DGTB_Tax_Amount , 0)   
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b  WITH (NOLOCK)     
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1008'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET CCessRate = isnull(DGTB_Tax_Rate , 0),     
CCessAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b  WITH (NOLOCK)     
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code    
and b.DGTB_Tax_Code ='ID1009'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET VGCCRate = isnull(DGTB_Tax_Rate , 0),     
VGCCAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'ID1010'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET DiscountRate = isnull(DGTB_Tax_Rate , 0),     
DiscountAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC001'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number    
  
UPDATE t        
SET PackingForwardingRate = isnull(DGTB_Tax_Rate , 0),     
PackingForwardingAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number    
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC008'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number   
  
UPDATE t        
SET InsuranceRate = isnull(DGTB_Tax_Rate , 0),     
InsuranceAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)       
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC009'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number   
  
UPDATE t        
SET FreightChargesRate = isnull(DGTB_Tax_Rate , 0),     
FreightChargesAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC010'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number   
  
UPDATE t        
SET OthersRate = isnull(DGTB_Tax_Rate , 0),     
OthersAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b  WITH (NOLOCK)     
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number       
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC011'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number   
  
UPDATE t        
SET TCSRate = isnull(DGTB_Tax_Rate , 0),     
TCSAmt = isnull(DGTB_Tax_Amount , 0)  
FROM #temp_Register t    
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register_Tax_Breakup  b WITH (NOLOCK)      
ON t.SourceDocumentNumber = b.DGTB_Register_Number        
AND t.ItemSerialNo = b.DGTB_Serial_Number        
AND t.ItemCode = b.DGTB_Item_Code     
and b.DGTB_Tax_Code = 'RC076'  
INNER JOIN EDWSTG40.TAX.FAS_D_GST_Register d WITH (NOLOCK)  
ON  b.DGTB_Register_Number = d.DGR_Register_Number        
AND b.DGTB_Serial_Number   = d.DGR_Serial_Number   
  
UPDATE tmp         
SET  HSNSACDesc = isnull(MHSN_Description,'')  
FROM #temp_Register tmp        
INNER JOIN EDWSTG40.MDM.GEM_M_HSN hsn WITH (NOLOCK) on MHSN_HSN_Code = HSNSACCode        
and MHSN_Country_Code = ToCountryCode and GorS = 'H';        
    
UPDATE tmp         
SET  HSNSACDesc = isnull(MSAC_Description, '')  
FROM #temp_Register tmp        
INNER JOIN EDWSTG40.MDM.GEM_M_Service_Accounts sac WITH (NOLOCK) on MSAC_SAC_Code = HSNSACCode        
and MSAC_Country_Code = ToCountryCode and GorS = 'S';    
   
   
UPDATE  tmp     -- by DSP on 30 Apr 2021    
SET tmp.StrICCode = IC.MCLED_Short_Description,    
tmp.StrRegionCode = region.MCLED_Short_Description,  
tmp.StrClusterCode = Cluster.MCLED_Short_Description  
FROM #temp_Register tmp        
INNER JOIN EDWSTG40.MDM.GEM_L_Job_Cluster_Elements JCE WITH (NOLOCK)  
ON tmp.JobCode = JCE.LJCE_Job_Code and JCE.LJCE_company_code = tmp.CompanyCode       
INNER JOIN EDWSTG40.MDM.GEM_M_Cluster_Element_Details IC WITH (NOLOCK)  
ON JCE.LJCE_IC_Code=IC.MCLED_CED_Code and IC.MCLED_company_code = tmp.CompanyCode       
INNER JOIN EDWSTG40.MDM.GEM_M_Cluster_Element_Details Cluster WITH (NOLOCK)  
ON JCE.LJCE_Cluster_Office_Code=Cluster.MCLED_CED_Code and IC.MCLED_company_code = tmp.CompanyCode       
INNER JOIN EDWSTG40.MDM.GEM_M_Cluster_Element_Details region  WITH (NOLOCK)  
ON JCE.LJCE_Region_Code=region.MCLED_CED_Code and IC.MCLED_company_code = tmp.CompanyCode;    
  
UPDATE tmp   
SET CurrencyDesc = concat(CUR.MCUR_Short_Description , ' - '  ,CUR.MCUR_Description )  
FROM #temp_Register tmp       
INNER JOIN      
EDWSTG40.MDM.GEM_M_Currencies CUR WITH (NOLOCK) on tmp.CurrencyCode = CUR.MCUR_Currency_Code;   
  
UPDATE tmp        
SET UOMDesc = UoM.MTD_Trans_Type_Short_Desc  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.COM.GEN_M_Transaction_Details UoM WITH (NOLOCK)  
on tmp.UOMCode = UoM.MTD_Trans_Detail_Code and MTD_Trans_Type_Code = 8012   
  
UPDATE tmp        
SET JobDesc = jobs.MJOB_Description   
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.mdm.GEM_M_Jobs jobs WITH (NOLOCK) on tmp.JobCode = jobs.MJOB_Job_Code;        
    
UPDATE tmp        
SET ACDesc = jobs.MJOB_Description  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.mdm.GEM_M_Jobs jobs WITH (NOLOCK) on tmp.ACCode = jobs.MJOB_Job_Code;        
    
UPDATE tmp        
SET BAName = BA.MBA_BA_Name  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.mdm.BAM_M_Business_Associates BA WITH (NOLOCK) on tmp.BACode = BA.MBA_BA_Code and BA.MBA_Company_Code = tmp.CompanyCode ;        
        
--Added By Neels on 25th Jul 2022 for Odish/Orrisa start  
 -- 20    -Orissa -499    Odisha  
 update #temp_Register SET SrcStateCode = 499 where  SrcStateCode  = 20  
  
 --506-    Andhra Pradesh (New)- 1    Andhra Pradesh  
 update #temp_Register SET SrcStateCode = 1 where SrcStateCode = 506  
  
 -- 20    -Orissa -499    Odisha  
 update #temp_Register SET DelStateCode = 499 where DelStateCode = 20  
  
 -- 506-    Andhra Pradesh (New)- 1    Andhra Pradesh  
 update #temp_Register SET DelStateCode = 1 where DelStateCode = 506  
--Added By Neels on 25th Jul 2022 for Odish/Orrisa start  
  
  
  
UPDATE tmp        
SET SrcStateName =  ST.MSTAT_Name  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.MDM.GEM_U_States ST WITH (NOLOCK) on tmp.SrcStateCode = ST.MSTAT_State_Code AND tmp.FromCountryCode = st.MSTAT_Country_Code    
  
UPDATE tmp        
SET DelStateName = DT.MSTAT_Name  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.MDM.GEM_U_States DT WITH (NOLOCK) ON tmp.DelStateCode = DT.MSTAT_State_Code AND tmp.ToCountryCode = Dt.MSTAT_Country_Code   
  
UPDATE tmp        
SET FromCountryName =  CT.MCOUN_Name  
FROM #temp_Register tmp        
INNER JOIN        
EDWSTG40.MDM.GEM_U_Countries CT WITH (NOLOCK) on tmp.FromCountryCode = CT.MCOUN_Country_Code    
  
UPDATE tmp        
SET ToCountryName = CT.MCOUN_Name  
FROM #temp_Register tmp        
INNER JOIN EDWSTG40.MDM.GEM_U_Countries CT WITH (NOLOCK) ON tmp.ToCountryCode = CT.MCOUN_Country_Code   
  
update tmp set tmp.BOLNo=HCDUT_Bill_Of_Ladding_Air_way_Number,  
tmp.BOLDate=HCDUT_Bill_Of_Ladding_Air_Way_Date,  
tmp.BOENo= isnull(HCDUT_Provisional_Customs_Bill_Of_Entry_Number,  
  HCDUT_Final_Customs_Bill_Of_Entry_Number)   
  from #temp_Register tmp   
  inner join EDWSTG40.whs.WRH_H_mrn a on tmp.SourceDocumentNumber=a.HMRN_MRN_Number  
  inner join EDWSTG40.whs.WRH_H_GIN b on a.HMRN_Gin_Number=b.HGIN_Gin_Number   
  inner join EDWSTG40.prc.IMP_d_Bill_Of_Entry c on b.HGIN_BOE_Number=c.dBOE_BOE_Number  
  inner join EDWSTG40.prc.IMP_H_CUSTOMS_DUTY d on c.dBOE_CD_Number=d.HCDUT_CD_Number  
   
   
         
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'ItemSerialNo')        
UPDATE #temp_Register SET ItemSerialNo = '' WHERE ItemSerialNo IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'UOM')     
UPDATE #temp_Register SET UOMCode = '' where UOMCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'Qty')        
UPDATE #temp_Register SET Qty = '' where Qty IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BRNo')        
UPDATE #temp_Register SET BRNo = '' where BRNo IS NULL         
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BRDate')        
UPDATE #temp_Register SET BRDate = '' where BRDate IS NULL         
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BRSLNO')        
UPDATE #temp_Register SET BRSLNO = '' where BRSLNO IS NULL         
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'SupplyType')        
UPDATE #temp_Register SET SupplyType = '' where SupplyType IS NULL ;  
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'SourceDocumentDate')   
UPDATE #temp_Register SET SourceDocumentDate = '' WHERE SourceDocumentDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns where TMP_Column = 'ACCode')        
UPDATE #temp_Register SET ACCode = '' where ACCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns where TMP_Column = 'InvoiceNo')        
UPDATE #temp_Register SET InvoiceNo = '' where InvoiceNo IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns where TMP_Column = 'InvoiceDate')        
UPDATE #temp_Register SET InvoiceDate = '' where InvoiceDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns where TMP_Column = 'OrderNo')        
UPDATE #temp_Register SET OrderNo = '' where OrderNo IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns where TMP_Column = 'AccountingPeriod')        
UPDATE #temp_Register SET AccountingPeriod = '' where AccountingPeriod IS NULL     
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'GSTIN')        
UPDATE #temp_Register SET GSTIN = '' where GSTIN IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BAGSTIN')        
UPDATE #temp_Register SET BAGSTIN = '' where BAGSTIN IS NULL    
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BACode')        
UPDATE #temp_Register SET BACode = '' where BACode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'JobCode')        
UPDATE #temp_Register SET JobCode = '' where JobCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'CurrencyCode')        
UPDATE #temp_Register SET CurrencyCode = '' where CurrencyCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'ItemCode')        
UPDATE #temp_Register SET ItemCode = '' where ItemCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'OrderType')        
UPDATE #temp_Register SET OrderType = '' where OrderType IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'GorS')        
UPDATE #temp_Register SET GorS = '' where GorS IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'HSNSACCode')        
UPDATE #temp_Register SET HSNSACCode = '' where HSNSACCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BOLDate')        
UPDATE #temp_Register SET BOLDate = '' where BOLDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'PortCode')        
UPDATE #temp_Register SET PortCode = '' where PortCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'JVNo')        
UPDATE #temp_Register SET JVNo = '' where JVNo IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'VoucherNo')        
UPDATE #temp_Register SET VoucherNo = '' where VoucherNo IS NULL    
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'VoucherDate')        
UPDATE #temp_Register SET VoucherDate = '' where VoucherDate IS NULL   
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'PortCode')        
UPDATE #temp_Register SET PortCode = '' where PortCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'MRNDate')        
UPDATE #temp_Register SET MRNDate = '' where MRNDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'Reco2A')        
UPDATE #temp_Register SET Reco2A = '' where Reco2A IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'RecoDate')        
UPDATE #temp_Register SET RecoDate = '' where RecoDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BRDate')        
UPDATE #temp_Register SET BRDate = '' where BRDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'WOBillDate')        
UPDATE #temp_Register SET WOBillDate = '' where WOBillDate IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'WOBillNo')        
UPDATE #temp_Register SET WOBillNo = '' where WOBillNo IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'InvoiceType')        
UPDATE #temp_Register SET InvoiceType = '' where InvoiceType IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'RefDTCode')        
UPDATE #temp_Register SET RefDTCode = '' where RefDTCode IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'DocType')        
UPDATE #temp_Register SET DocType = '' where DocType IS NULL        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'ASPExprtd')        
UPDATE #temp_Register SET ASPExprtd = '' where ASPExprtd IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'DocStatus')        
UPDATE #temp_Register SET DocStatus = '' where DocStatus IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'SrcStateCode')        
UPDATE #temp_Register SET SrcStateCode = '' where SrcStateCode IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'DelStateCode') 
UPDATE #temp_Register SET DelStateCode = '' where DelStateCode IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'ITC')        
UPDATE #temp_Register SET ITC = '' where ITC IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'RCM')        
UPDATE #temp_Register SET RCM = '' where RCM IS NULL;        
IF EXISTS (SELECT TOP 1 1 FROM #Tmp_Columns WHERE TMP_Column = 'BOENo')        
UPDATE #temp_Register SET BOENo = '' where BOENo IS NULL;        
    
  --Added on 20th Jul 2022 by Neels to delete duplicates in Purchase register -- start---------  
select distinct InvoiceNo dgr_invoice_number   
into #Temp_invoice_Documents  
from #temp_Register    
where CompanyCode in (SELECT strCompApplicable FROM #TempCompApplicabilityMaster) and BRType = 7  
  and OrderNo is not null and isnull(ACCode,'') <> ''  
  and len(InvoiceNo) = 16  
  
--drop table if exists #Temp_Documents_invoiceno  
select a.InvoiceNo, count(distinct a.SourceDocumentNumber) doc_count  
into #Temp_Documents_invoiceno  
from #temp_Register a, #Temp_invoice_Documents b  
where a.InvoiceNo = b.dgr_invoice_number  
  and CompanyCode in (SELECT strCompApplicable FROM #TempCompApplicabilityMaster)  and BRType = 7  
  and SourceDocumentNumber not like '%pvc%'  
group by a.InvoiceNo  
having count(distinct a.SourceDocumentNumber) > 1  
  
--select * from #temp_Register  
delete from #temp_Register  
where exists (select 'x' from #Temp_Documents_invoiceno c  
where #temp_Register.InvoiceNo = c.InvoiceNo  
  and CompanyCode in (SELECT strCompApplicable FROM #TempCompApplicabilityMaster) and BRType = 7  
  and SourceDocumentNumber like '%mrn%')  
--Added on 20th Jul 2022 to delete duplicates in Purchase register -- end-----------  
  
--Added By Neels on 22Jul2022 for inv type start--  
 ALTER TABLE #temp_Register ADD Irninvoicetype VARCHAR(15);  
 ALTER TABLE #temp_Register ADD natureoftrans VARCHAR(15);  
 ALTER TABLE #temp_Register ADD buyerstatecode VARCHAR(15);  
 ALTER TABLE #temp_Register ADD placeofsupply VARCHAR(15);  
  
 UPDATE #temp_Register   
 SET  buyerstatecode = 96, placeofsupply = 96   
 WHERE ToCountryCode != 100 AND BRType = 1;   
  
 UPDATE #temp_Register   
 SET  buyerstatecode = DelStateCode, placeofsupply = DelStateCode   
 WHERE ToCountryCode  = 100 AND BRType = 1;   
  
 UPDATE #temp_Register   
 SET  natureoftrans = 'INTRA'        
 WHERE SrcStateCode = DelStateCode AND BRType = 1;        
        
 UPDATE #temp_Register   
 SET  natureoftrans = 'INTER'        
 WHERE SrcStateCode != DelStateCode AND BRType = 1;   
  
 UPDATE #temp_Register   
 SET  Irninvoicetype = 'B2B'   
 WHERE BAGSTIN is not null AND SUBSTRING(BAGSTIN,1,1) not in ('U')  
   AND BRType = 1;        
        
 UPDATE #temp_Register   
 SET  Irninvoicetype = 'B2CL'           
 WHERE isnull(BAGSTIN,'') = '' or SUBSTRING(BAGSTIN,1,1) in ('U')   
   AND TaxableAmt > 250000 AND natureoftrans = 'INTER'  
   AND BRType = 1;      
  
 UPDATE #temp_Register   
 SET  Irninvoicetype = 'B2CS'           
 WHERE isnull(BAGSTIN,'') ='' or SUBSTRING(BAGSTIN,1,1) in ('U')   
   AND TaxableAmt <= 250000  
   AND BRType = 1;         
  
 UPDATE #temp_Register   
 SET  Irninvoicetype = 'B2CS'           
 WHERE isnull(BAGSTIN,'') ='' or SUBSTRING(BAGSTIN,1,1) in ('U')   
   AND natureoftrans = 'INTRA' AND BRType = 1;    
     
 --UPDATE #temp_Register   
 --SET  Irninvoicetype='B2B'         
 --WHERE Irninvoicetype in ('B2CL','B2CS')   
 --  AND buyerstatecode=96;  
  
   --select * from FIN.TAX_T_Register_Report  
        
 --UPDATE #temp_Register   
 --SET  Irninvoicetype='EXWOP'        
 --FROM #temp_Register a,   
 --  EDWSTG40.CIM.INV_H_Invoice,  
 --  EDWSTG40.CIM.ORD_M_Invoice_Address, EDWSTG40.COM.GEN_M_Address_Book        
 --WHERE HINV_Actual_Invoice_No = InvoiceNo  
 --  AND HINV_Consignee_Location_Code = COS_Invoice_Address_Code AND COS_Invoice_Address_Tag = 'L'   
 --  AND buyerstatecode=96 AND MAB_AB_Code = COS_AB_Code;  
  
 UPDATE #temp_Register   
 SET  Irninvoicetype = 'EXWOP'         
 WHERE buyerstatecode = 96 AND BRType = 1;  
  
 UPDATE #temp_Register     
 SET  Irninvoicetype = 'SEWOP'  
 WHERE DelStateCode IN (10131,10132,10133,10134,10135,  
      10136,10137,10138,10139,10140,10141,10142,10143,  
      10144,10145,10146,10147,10148,10149,10150,10151,  
      10152,10153,10154,10155,10156,10157,10158,10159,  
      10160,10161,10162,10163,10164,10165,10166,10167,  
      10168,10169,10170) AND BRType = 1;  
  
 --UPDATE #temp_Register     
 --SET  Irninvoicetype = 'B2CS'  
 --WHERE actualInvType = 'XL';  
        
 --UPDATE #temp_Register   
 --SET  Irninvoicetype = 'B2B'        
 --WHERE Irninvoicetype in ('B2CL','B2CS')   
 --  AND @CustomerNature IN ('CD','CG','CU','MU','OG','SG','SU')    
--Added By Neels on 22Jul2022 for inv type end--  
  
  
 UPDATE #temp_Register  
 SET  invoiceGroup = HINV_Invoice_Group, InternalInvNo = HINV_Invoice_No,  
   ActualInvoiceType = HINV_ActualInv_Type  , Certification_date = HINV_Certification_Date
 FROM EDWSTG40.CIM.INV_H_Invoice  
 WHERE HINV_Job_Code = JobCode AND HINV_Invoice_Type = InvoiceType  
   AND HINV_Invoice_No = SUBSTRING(RIGHT(SourceDocumentNumber , LEN(SourceDocumentNumber) - CHARINDEX('-', SourceDocumentNumber)), 1,   
   CHARINDEX('-', RIGHT(SourceDocumentNumber , LEN(SourceDocumentNumber) - CHARINDEX('-', SourceDocumentNumber)))-1)  
   AND BRType = 1;  

   --DELETE	FROM #temp_Register
   --where	BRType = 1 AND ActualInvoiceType = 'JW';

   --DELETE	FROM #temp_Register
   --WHERE	BRType = 1 AND substring(InvoiceNo, 9, 3) = 'OTS';
  
 UPDATE #temp_Register  
 SET  Qty = IIF(Qty < 0, Qty*-1, Qty),  
   BasicRate = IIF(BasicRate < 0, BasicRate*-1, BasicRate),  
   TaxableAmt = IIF(TaxableAmt < 0, TaxableAmt*-1, TaxableAmt),  
   TaxAmt = IIF(TaxAmt < 0, TaxAmt*-1, TaxAmt),  
   TotalAmt = IIF(TotalAmt < 0, TotalAmt*-1, TotalAmt),  
   IGSTAmt = IIF(IGSTAmt < 0, IGSTAmt*-1, IGSTAmt),  
   CGSTAmt = IIF(CGSTAmt < 0, CGSTAmt*-1, CGSTAmt),  
   SGSTAmt = IIF(SGSTAmt < 0, SGSTAmt*-1, SGSTAmt),  
   UTGSTAmt = IIF(UTGSTAmt < 0, UTGSTAmt*-1, UTGSTAmt),  
   RIGSTAmt = IIF(RIGSTAmt < 0, RIGSTAmt*-1, RIGSTAmt),  
   RCGSTAmt = IIF(RCGSTAmt < 0, RCGSTAmt*-1, RCGSTAmt),  
   RSGSTAmt = IIF(RSGSTAmt < 0, RSGSTAmt*-1, RSGSTAmt),  
   RUTGSTAmt = IIF(RUTGSTAmt < 0, RUTGSTAmt*-1, RUTGSTAmt),  
   CCessAmt = IIF(CCessAmt < 0, CCessAmt*-1, CCessAmt),  
   VGCCAmt = IIF(VGCCAmt < 0, VGCCAmt*-1, VGCCAmt),  
   RCMGCCAmt = IIF(RCMGCCAmt < 0, RCMGCCAmt*-1, RCMGCCAmt),  
   DiscountAmt = IIF(DiscountAmt < 0, DiscountAmt*-1, DiscountAmt),  
   PackingForwardingAmt = IIF(PackingForwardingAmt < 0, PackingForwardingAmt*-1, PackingForwardingAmt),  
   InsuranceAmt = IIF(InsuranceAmt < 0, InsuranceAmt*-1, InsuranceAmt),  
   FreightChargesAmt = IIF(FreightChargesAmt < 0, FreightChargesAmt*-1, FreightChargesAmt),  
   OthersAmt = IIF(OthersAmt < 0, OthersAmt*-1, OthersAmt),  
   TCSAmt = IIF(TCSAmt < 0, TCSAmt*-1, TCSAmt),  
   AdvAdjIGSTAmt = IIF(AdvAdjIGSTAmt < 0, AdvAdjIGSTAmt*-1, AdvAdjIGSTAmt),  
   AdvAdjCGSTAmt = IIF(AdvAdjCGSTAmt < 0, AdvAdjCGSTAmt*-1, AdvAdjCGSTAmt),  
   AdvAdjSGSTAmt = IIF(AdvAdjSGSTAmt < 0, AdvAdjSGSTAmt*-1, AdvAdjSGSTAmt),  
   AdvAdjUTGSTAmt = IIF(AdvAdjUTGSTAmt < 0, AdvAdjUTGSTAmt*-1, AdvAdjUTGSTAmt)  
 WHERE invoiceGroup = 'SR' AND BRType = 1;  
  
 --for gstr 2B Start--  
   
 --drop table if exists #Temp_JV_Details  
  
 UPDATE #temp_Register  
 SET  JV_No = TVGHJR_JV_Number,  
   FJV_Number = TVGHJR_FJV_Number,  
   FJV_Date = TVGHJR_FJV_Date  
 FROM EDWSTG40.STG.FAS_T_Vendor_GST_Hold_JV_Request  
 WHERE TVGHJR_LR_Number = LR_Number AND BRType <> 1;  
  
 UPDATE #temp_Register   
 SET  PJV_SJV_Date = HPURJ_voucher_date 
 FROM EDWSTG40.ACP.FAS_H_Purchase_Journals  
 WHERE HPURJ_PJV_Number = JV_No  
   and HPURJ_LR_Number = LR_Number AND BRType <> 1;  

 UPDATE #temp_Register   
 SET  PJV_SJV_Date = HPURJ_voucher_date  
 FROM EDWSTG40.ACPARC.FAS_H_Purchase_Journals  
 WHERE HPURJ_PJV_Number = JV_No  
   and HPURJ_LR_Number = LR_Number AND BRType <> 1;
  
 UPDATE #temp_Register   
 SET  PJV_SJV_Date = HSCJ_voucher_date  
 FROM EDWSTG40.ACP.FAS_H_Subcontractor_Journals  
 WHERE HSCJ_SJV_Number = JV_No   
   and HSCJ_LR_Number = LR_Number AND BRType <> 1;  

 UPDATE #temp_Register   
 SET  PJV_SJV_Date = HSCJ_voucher_date  
 FROM EDWSTG40.ACPARC.FAS_H_Subcontractor_Journals  
 WHERE HSCJ_SJV_Number = JV_No   
   and HSCJ_LR_Number = LR_Number AND BRType <> 1; 
     
   
 UPDATE #temp_Register  
 SET  GSTR2BITCStatus = CASE WHEN TLREG_Is_ITC_Hold = 'N' AND TLREG_DS_Code in (7, 19) THEN 'Yes'  
    WHEN TLREG_Is_ITC_Hold = 'Y' And FJV_Number Is Not Null THEN 'Yes'  
    END, -- AS 'GSTR- 2B status (ITC Availability)',  
   GSTR2B_Month = CASE WHEN TLREG_Is_ITC_Hold = 'N' And TLREG_DS_Code in (7, 19) THEN PJV_SJV_Date  
    WHEN TLREG_Is_ITC_Hold = 'Y' And FJV_Number Is Not Null THEN FJV_Date  
    END  
 FROM EDWSTG40.ACP.FAS_T_Ledger_Register  
 WHERE LR_Number = TLREG_LR_Number AND BRType <> 1;  
   --Left Join #Temp_JV_Details On TVGHJR_LR_Number = TLREG_LR_Number  
  
 --Select Distinct TVGHJR_LR_Number, TVGHJR_JV_Number, TVGHJR_FJV_Number, TVGHJR_FJV_Date  
 --Into #Temp_JV_Details  
 --From STG.FAS_T_Vendor_GST_Hold_JV_Request  
 --Where TVGHJR_LR_Number = 'LE/SZ000010/FPI/22/INR/0873863'--  
  
 --alter table #Temp_JV_Details add PJV_SJV_Date Date  
  
 --for gstr 2B End--  
  
 UPDATE #temp_Register  
 SET  BAGSTINAsPerEBR = DLRVB_BA_GSTIN_Number,  
   CompanyGSTINAsPerEBR = DLRVB_GSTIN_Number  
 FROM EDWSTG40.ACP.FAS_D_Ledger_Register_Vendor_bills WITH (NOLOCK)  
    WHERE LR_Number = DLRVB_LR_Number AND DLRVB_Serial_Number = 1  
   AND DLRVB_Company_Code in (SELECT strCompApplicable FROM #TempCompApplicabilityMaster) AND BRType <> 1;  



  
  
INSERT INTO FIN.TAX_T_Register_Report(TRR_Company_Code, TRR_BR_Type, TRR_AccountingPeriod, TRR_ACCode, TRR_ACDesc,  
  TRR_CurrencyCode, TRR_CurrencyDesc, TRR_SourceDocumentNumber, TRR_SourceDocumentDate,  
  TRR_InvoiceNo, TRR_InvoiceDate, TRR_InvoiceType,  
  TRR_OrderNo, TRR_OrderDate, TRR_OrderType, TRR_OrderTypeDesc,  
  TRR_JobCode, TRR_JobDesc,  
  TRR_BACode, TRR_BAName,  
  TRR_ItemSerialNo,  
  TRR_ItemCode, TRR_ItemDesc,  
  TRR_UOMCode, UOMDesc,  
  TRR_Qty, TRR_BasicRate, TRR_TaxableAmt, TRR_TaxAmt, TRR_TotalAmt,  
  TRR_GSTIN, TRR_BAGSTIN,  
  TRR_FromCountryCode, TRR_FromCountryName,  
  TRR_ToCountryCode, TRR_ToCountryName,  
  TRR_SrcStateCode, TRR_SrcStateName,  
  TRR_DelStateCode, TRR_DelStateName,  
  TRR_IGSTRate, TRR_IGSTAmt,  
  TRR_CGSTRate, TRR_CGSTAmt,  
  TRR_SGSTRate, TRR_SGSTAmt,  
  TRR_UTGSTRate, TRR_UTGSTAmt,  
  TRR_RIGSTRate, TRR_RIGSTAmt,  
  TRR_RCGSTRate, TRR_RCGSTAmt,  
  TRR_RSGSTRate, TRR_RSGSTAmt,  
  TRR_RUTGSTRate, TRR_RUTGSTAmt,  
  TRR_CCessRate, TRR_CCessAmt,  
  TRR_VGCCRate, TRR_VGCCAmt,  
  TRR_RCMGCCRate, TRR_RCMGCCAmt,  
  TRR_DiscountRate, TRR_DiscountAmt,  
  TRR_PackingForwardingRate, TRR_PackingForwardingAmt,  
  TRR_InsuranceRate, TRR_InsuranceAmt,  
  TRR_FreightChargesRate, TRR_FreightChargesAmt,  
  TRR_OthersRate, TRR_OthersAmt,  
  TRR_TCSRate, TRR_TCSAmt,  
  TRR_AdvAdjIGSTRate, TRR_AdvAdjIGSTAmt,  
  TRR_AdvAdjCGSTRate, TRR_AdvAdjCGSTAmt,  
  TRR_AdvAdjSGSTRate, TRR_AdvAdjSGSTAmt,  
  TRR_AdvAdjUTGSTRate, TRR_AdvAdjUTGSTAmt,  
  TRR_ITC, TRR_RCM,  
  TRR_Reco2A, TRR_RecoDate,  
  TRR_GorS,  
  TRR_HSNSACCode, TRR_HSNSACDesc,  
  TRR_ServiceCode, TRR_ServiceDesc,  
  TRR_JVNo, TRR_VoucherNo, TRR_VoucherDate,  
  TRR_BRNo, TRR_BRSLNO, TRR_BRDate,  
  TRR_MRNNo, TRR_MRNDate,  
  TRR_WOBillNo, TRR_WOBillDate,  
  TRR_RefDTCode,  
  TRR_IRNNumber, TRR_AcknowledgeNumber, TRR_AcknowledgeDate,  
  TRR_EwayBillNumber, TRR_EwayBillDate,  
  TRR_BOLNo, TRR_BOLDate, TRR_PortCode, BOENo,  
  TRR_SupplyType, TRR_ASPExprtd, TRR_DocType, TRR_DocStatus,  
  TRR_StrICCode, TRR_StrRegionCode, TRR_StrClusterCode, TRR_Debit_Job, TRR_Invoice_Type, 
  TRR_LR_Number, TRR_GST_Release_Mode, TRR_GSTR2B_ITC_Status, TRR_GSTR2B_Month,  
  TRR_FJV_Date, TRR_FJV_Number, TRR_BAGSTINAsPerEBR, TRR_CompanyGSTINAsPerEBR,  
  TRR_ActualInvoiceType , TRR_POS_Code , TRR_Certification_date  , TRR_TaxPayer_Category)   
  
  SELECT CompanyCode, BRType, AccountingPeriod, ACCode, ACDesc,  
  CurrencyCode, CurrencyDesc,   
  SourceDocumentNumber, SourceDocumentDate,  
  InvoiceNo, InvoiceDate, InvoiceType,  
  OrderNo, OrderDate, OrderType, OrderTypeDesc,  
  JobCode, JobDesc,  
  BACode, BAName,   
  ItemSerialNo,   
  ItemCode, ItemDesc,  
  UOMCode, UOMDesc,  
  Qty,BasicRate,TaxableAmt,TaxAmt,TotalAmt,  
  GSTIN, BAGSTIN,  
  FromCountryCode, FromCountryName,  
  ToCountryCode, ToCountryName,  
  SrcStateCode, SrcStateName,  
  DelStateCode, DelStateName,  
  IGSTRate, IGSTAmt,  
  CGSTRate, CGSTAmt,  
  SGSTRate, SGSTAmt,  
  UTGSTRate, UTGSTAmt,  
  RIGSTRate, RIGSTAmt,  
  RCGSTRate, RCGSTAmt,  
  RSGSTRate, RSGSTAmt,  
  RUTGSTRate, RUTGSTAmt,  
  CCessRate, CCessAmt,  
  VGCCRate, VGCCAmt,  
  RCMGCCRate, RCMGCCAmt,  
  DiscountRate, DiscountAmt,  
  PackingForwardingRate, PackingForwardingAmt,  
  InsuranceRate, InsuranceAmt,  
  FreightChargesRate, FreightChargesAmt,  
  OthersRate, OthersAmt,  
  TCSRate, TCSAmt,  
  AdvAdjIGSTRate, AdvAdjIGSTAmt,  
  AdvAdjCGSTRate, AdvAdjCGSTAmt,  
  AdvAdjSGSTRate, AdvAdjSGSTAmt,  
  AdvAdjUTGSTRate, AdvAdjUTGSTAmt,  
  ITC, RCM,  
  Reco2A, RecoDate,  
  GorS,  
  HSNSACCode, HSNSACDesc,  
  ServiceCode, ServiceDesc,  
  JVNo, VoucherNo, VoucherDate,  
  BRNo, BRSLNO, BRDate,   
  MRNNo, MRNDate,  
  WOBillNo, WOBillDate,  
  RefDTCode,  
  IRNNumber, AcknowledgeNumber, AcknowledgeDate,  
  EwayBillNumber, EwayBillDate,  
  BOLNo, BOLDate, PortCode, BOENo,  
  SupplyType, ASPExprtd, DocType, DocStatus,  
  StrICCode, StrRegionCode, StrClusterCode, DebitJobCode, Irninvoicetype,  
  LR_Number, GSTReleaseMode,  GSTR2BITCStatus, GSTR2B_Month, FJV_Date,  
  FJV_Number, BAGSTINAsPerEBR, CompanyGSTINAsPerEBR,   
  ActualInvoiceType, 
  CASE WHEN BRType = 1 THEN buyerstatecode ELSE DelStateCode END, 
  Certification_date , TaxpayerCategory
FROM #temp_Register  
WHERE SourceDocumentNumber NOT like '%FGJ%';    
  
--SELECT * FROM FIN.TAX_T_Register_Report  
--WHERE TRR_SourceDocumentDate >= @lFromDate     
--  AND TRR_SourceDocumentDate <= @lToDate;  
       
DROP TABLE IF EXISTS #temp_Register;   
        
SET NOCOUNT OFF;     
    
END;

