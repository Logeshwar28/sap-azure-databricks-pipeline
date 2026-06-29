## Day 1 — ADF Bronze Layer Ingestion ✅

### Architecture
SAP Source (simulated) → Landing Zone (ADLS) → 
ADF Pipeline → Bronze Layer (ADLS)

### What I Built
- Parameterized ADF pipeline using ForEach activity 
  for dynamic multi-table ingestion
- Pipeline Variables (Array) driving dynamic file 
  iteration instead of hardcoded activities
- Landing Zone → Bronze Layer separation following 
  Medallion Architecture pattern
- Schema drift handling across 6 SAP tables with 
  different column structures (VBAK, VBAP, KNA1, 
  MARA, BKPF, BSEG)

### Tech Stack
Azure Data Factory | Azure Data Lake Storage Gen2 | 
ADF ForEach + Parameterized Datasets

### Challenges Solved
- MappingColumnNameNotFoundInSourceFile error — 
  resolved by clearing hardcoded schema mapping 
  to support schema drift across files
