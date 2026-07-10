## Project Overview
End-to-end SAP data engineering pipeline built on Azure,
simulating real enterprise data flow from SAP ERP system
to business-ready analytics layer using Azure Databricks
and Medallion Architecture.

## Architecture Diagram
[Add draw.io screenshot here]

SAP Source (Simulated)
      ↓
Landing Zone (ADLS Gen2)
      ↓ [Azure Data Factory — ForEach Pipeline]
Bronze Layer (Raw Delta Tables)
      ↓ [Databricks PySpark Notebooks]
Silver Layer (Cleaned & Transformed)
      ↓ [Business Logic + Joins]
Gold Layer (Aggregated — Business Ready)
      ↓
Unity Catalog (Governance)

## Key Features
- **Parameterized ADF Pipeline**: ForEach activity 
  dynamically processes 6 SAP tables — no hardcoding
- **Zero-credential notebooks**: Managed Identity + 
  Unity Catalog External Location (no secrets in code)
- **Data Quality**: DLT expectations enforce 99.6% 
  data quality with automatic bad record handling
- **Full Orchestration**: Databricks Workflows with 
  task dependencies + daily scheduling
- **CI/CD**: GitHub Actions auto-deploys notebooks 
  to Databricks in 15 seconds on every push

## SAP Tables Processed
| Table | Description | Records |
|-------|-------------|---------|
| VBAK  | Sales Order Header | 66,886 |
| VBAP  | Sales Order Items  | 67,429 |
| KNA1  | Customer Master    | 20,209 |
| MARA  | Material Master    | 23,897 |
| BKPF  | Accounting Header  | 150,057|
| BSEG  | Accounting Items   | 332,106|
| **Total** | | **660,584** |

## Gold Layer Outputs
| Table | Description |
|-------|-------------|
| gold_sales_orders | Sales orders with customer details |
| gold_product_performance | Material revenue analytics |
| gold_finance_summary | Financial document summary |

## Challenges Solved
- **Unity Catalog ADLS auth**: Resolved using Azure 
  Access Connector + Managed Identity instead of 
  hardcoded credentials
- **Schema drift**: Handled dynamic column mapping 
  across 6 SAP tables with different structures
- **DLT data quality**: Implemented expect_or_drop 
  to handle NULL customer records (0.4% dropped)

## How to Run
1. Clone repo
2. Configure Azure resources (ADLS, ADF, Databricks)
3. Run ADF pipeline: pl_sap_bronze_ingestion
4. Trigger Databricks Workflow: SAP_Pipeline_Orchestration
5. Or push to main branch — CI/CD auto-deploys!

## Author
Logeshwaran Kanagaraj
Azure Data Engineer | DP-700 Certified
LinkedIn: linkedin.com/in/logeshwaran-kanagaraj
GitHub: github.com/Logeshwar28
