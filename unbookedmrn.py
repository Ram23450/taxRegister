from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TAX_SP_TXR_Register_Report_Insert") \
    .getOrCreate()

# Set parameters
FromDate = '01-Sep-2021'
ToDate = '30-Sep-2021'

# Load necessary tables (assuming they are already registered as Spark tables)
gen_m_dt_validations = spark.table("EDWSTG40.COM.GEN_M_DT_Validations")
fas_h_gst_register = spark.table("EDWSTG40.TAX.FAS_H_GST_Register")
fas_d_gst_register = spark.table("EDWSTG40.TAX.FAS_D_GST_Register")
fas_d_gst_register_general = spark.table("EDWSTG40.TAX.FAS_D_GST_Register_General")
stm_m_tax_nature = spark.table("EDWSTG40.MDM.STM_M_Tax_Nature")

# Set CompTaxApplicability
comp_tax_applicability = gen_m_dt_validations \
    .filter((gen_m_dt_validations["MDTV_DT_Code"] == 30179) & (gen_m_dt_validations["MDTV_DOPT_Code"] == 'V') & 
            (gen_m_dt_validations["MDTV_Validation_Field"] == 'TaxRegApplicability')) \
    .select("MDTV_Comparable_Validation_Field") \
    .first()[0]

# Create temporary table for CompTaxApplicability
comp_tax_applicability_df = spark.createDataFrame([(comp_tax_applicability,)], ["strCompApplicable"])
comp_tax_applicability_df.createOrReplaceTempView("TempCompApplicabilityMaster")

# Filter and process data
temp_register_df = fas_h_gst_register.join(fas_d_gst_register, 
                                           fas_h_gst_register["HGR_Register_Number"] == fas_d_gst_register["DGR_Register_Number"]) \
                                      .join(fas_d_gst_register_general, 
                                            fas_h_gst_register["HGR_Register_Number"] == fas_d_gst_register_general["DGRG_Register_Number"]) \
                                      .join(stm_m_tax_nature, 
                                            (stm_m_tax_nature["MTN_Country_Code"] == fas_d_gst_register["DGR_To_Country_Code"]) & 
                                            (stm_m_tax_nature["MTN_Code"] == fas_d_gst_register["DGR_Service_Code"]) & 
                                            (stm_m_tax_nature["MTN_Tax_Type"] == 101), "left") \
                                      .filter((fas_h_gst_register["HGR_Register_Date"] >= FromDate) & 
                                              (fas_h_gst_register["HGR_Register_Date"] <= ToDate) & 
                                              (~fas_h_gst_register["HGR_Register_Number"].like("%FGJ%"))) \
                                      .select(fas_h_gst_register["HGR_Company_Code"], fas_h_gst_register["HGR_BR_Type_Code"], 
                                              fas_h_gst_register["HGR_Register_Number"].alias("SourceDocumentNumber"), 
                                              fas_h_gst_register["HGR_Register_Date"].alias("SourceDocumentDate"), 
                                              fas_h_gst_register["HGR_AC_Code"].alias("ACCode"), 
                                              fas_d_gst_register["DGR_Invoice_Number"].alias("InvoiceNo"), 
                                              fas_d_gst_register["DGR_Invoice_Date"].alias("InvoiceDate"), 
                                              fas_h_gst_register["HGR_Order_Number"].alias("OrderNo"), 
                                              fas_h_gst_register["HGR_Accounting_Period"], 
                                              fas_h_gst_register["HGR_GSTIN_Number"].alias("GSTIN"), 
                                              fas_h_gst_register["HGR_BA_GSTIN_Number"].alias("BAGSTIN"), 
                                              fas_h_gst_register["HGR_BA_Code"].alias("BACode"), 
                                              fas_h_gst_register["HGR_Job_Code"].alias("JobCode"), 
                                              fas_h_gst_register["HGR_Currency_Code"].alias("CurrencyCode"), 
                                              fas_d_gst_register["DGR_Serial_Number"].alias("ItemSerialNo"), 
                                              fas_d_gst_register["DGR_ITEM_Code"].alias("ItemCode"), 
                                              fas_d_gst_register["DGR_UOM_Code"].alias("UOMCode"), 
                                              fas_d_gst_register["DGR_Qty"].alias("Qty"), 
                                              fas_d_gst_register["DGR_Basic_Rate"].alias("BasicRate"), 
                                              fas_d_gst_register["DGR_Amount"].alias("TaxableAmt"), 
                                              fas_d_gst_register["DGR_Tax_Amount"].alias("TaxAmt"), 
                                              fas_d_gst_register["DGR_Total_Amount"].alias("TotalAmt"), 
                                              fas_d_gst_register_general["DGRG_Order_Details_Code"].alias("OrderType"), 
                                              fas_d_gst_register["DGR_From_Country_Code"].alias("FromCountryCode"), 
                                              fas_d_gst_register["DGR_From_State_Code"].alias("SrcStateCode"), 
                                              fas_d_gst_register["DGR_To_Country_Code"].alias("ToCountryCode"), 
                                              fas_d_gst_register["DGR_To_State_Code"].alias("DelStateCode"), 
                                              fas_d_gst_register["DGR_GorS_Tag"].alias("GorS"), 
                                              fas_d_gst_register["DGR_HSN_SAC_Code"].alias("HSNSACCode"), 
                                              fas_d_gst_register_general["DGRG_BOE_Number"].alias("BOLNo"), 
                                              fas_d_gst_register_general["DGRG_BOE_Date"].alias("BOLDate"), 
                                              fas_d_gst_register_general["DGRG_PORT_Code"].alias("PortCode"), 
                                              fas_h_gst_register["HGR_JV_Reference_Number"].alias("JVNo"), 
                                              fas_h_gst_register["HGR_Voucher_Number"].alias("VoucherNo"), 
                                              fas_h_gst_register["HGR_Voucher_Date"].alias("VoucherDate"), 
                                              fas_d_gst_register["DGR_Reonciliation_Code"].isNull().otherwise('N').alias("Reco2A"), 
                                              fas_d_gst_register["DGR_Reconciliation_Date"].alias("RecoDate"), 
                                              fas_d_gst_register_general["DGRG_BR_Number"].alias("BRNo"), 
                                              fas_d_gst_register_general["DGRG_BR_Date"].alias("BRDate"), 
                                              fas_d_gst_register_general["DGRG_MRN_Number"].alias("MRNNo"), 
                                              fas_d_gst_register_general["DGRG_MRN_Date"].alias("MRNDate"), 
                                              fas_d_gst_register_general["DGRG_WO_Bill_Number"].alias("WOBillNo"), 
                                              fas_d_gst_register_general["DGRG_WO_Bill_Date"].alias("WOBillDate"), 
                                              fas_d_gst_register_general["DGRG_Invoice_Type"].alias("InvoiceType"), 
                                              fas_h_gst_register["HGR_Reference_DT_Code"].alias("RefDTCode"), 
                                              fas_d_gst_register["DGR_Document_Type"].alias("DocType"), 
                                              fas_d_gst_register["DGR_ITC_Applicable"].alias("ITC"), 
                                              fas_d_gst_register["DGR_RCM_Applicable"].alias("RCM"), 
                                              fas_d_gst_register_general["DGRG_BOE_Number"], 
                                              fas_d_gst_register["DGR_Service_Code"], 
                                              stm_m_tax_nature["MTN_Description"].alias("ServiceDesc"), 
                                              fas_d_gst_register_general["DGRG_IRN_Number"], 
                                              fas_d_gst_register_general["DGRG_Acknowledgement_Number"], 
                                              fas_d_gst_register_general["DGRG_Acknowledgement_Date"].alias("AcknowledgeDate"), 
                                              fas_d_gst_register["DGR_Debit_Job_Code"], 
                                              fas_h_gst_register["HGR_LR_Number"], 
                                              fas_h_gst_register["HGR_GST_Release_Mode"])

# Perform required operations on temp_register_df

# Save the result to desired location
temp_register_df.write.mode("overwrite").parquet("path_to_save_result")

# Stop Spark session
spark.stop()
