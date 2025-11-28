# 🚦 ETL Toll Data Pipeline — Apache Airflow  + PostgreSQL
A complete end-to-end ETL pipeline that ingests raw toll transaction data from multiple file formats (CSV, TSV & Fixed Width), processes and transforms it using Airflow PythonOperators, and loads the final curated dataset into a PostgreSQL fact table.

This project is fully containerized using AWS MWAA Local Runner (Airflow 2.10.3) and includes integration with a local Postgres database for easy development and testing.

## 📁 Project Structure
<img width="699" height="812" alt="Image" src="https://github.com/user-attachments/assets/f2312bd7-df90-4c7a-8a4b-81685777cfe7" />

## Technology Stack
Component                                      Description
Apache Airflow (MWAA Local Runner)             Manages and orchestrates the ETL
PythonOperators                                Implements ETL logic
PostgreSQL                                     Final destination for toll fact table
Docker Compose                                 Full reproducible pipeline
Requests / tarfile / CSV                       Core data extraction libraries

## 👉 Data Sources
The ETL downloads a TGZ bundle containing:

1️⃣ Vehical-data.csv
<html xmlns:v="urn:schemas-microsoft-com:vml"
xmlns:o="urn:schemas-microsoft-com:office:office"
xmlns:x="urn:schemas-microsoft-com:office:excel"
xmlns="http://www.w3.org/TR/REC-html40">

<head>

<meta name=ProgId content=Excel.Sheet>
<meta name=Generator content="Microsoft Excel 15">
<link id=Main-File rel=Main-File
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip.htm">
<link rel=File-List
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_filelist.xml">

<!--table
	{mso-displayed-decimal-separator:"\.";
	mso-displayed-thousand-separator:"\,";}
@page
	{margin:.75in .7in .75in .7in;
	mso-header-margin:.3in;
	mso-footer-margin:.3in;}
tr
	{mso-height-source:auto;}
col
	{mso-width-source:auto;}
br
	{mso-data-placement:same-cell;}
td
	{padding-top:1px;
	padding-right:1px;
	padding-left:1px;
	mso-ignore:padding;
	color:black;
	font-size:12.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"Aptos Narrow", sans-serif;
	mso-font-charset:0;
	mso-number-format:General;
	text-align:general;
	vertical-align:bottom;
	border:none;
	mso-background-source:auto;
	mso-pattern:auto;
	mso-protection:locked visible;
	white-space:nowrap;
	mso-rotate:0;}
.xl65
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl66
	{border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl67
	{border-top:none;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl68
	{border-top:none;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl69
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:none;
	border-left:none;}
.xl70
	{border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:none;
	border-left:.5pt solid windowtext;}
-->
</head>

<body link="#467886" vlink="#96607D">
<meta charset=utf-8>


Component | Description
-- | --
Apache   Airflow (MWAA Local Runner) | Manages and orchestrates the ETL
PythonOperators | Implements ETL logic
PostgreSQL | Final destination for toll fact table
Docker   Compose | Full reproducible pipeline
Requests   / tarfile / CSV | Core data extraction libraries



</body>

</html>

<br><br>

2️⃣ Tollplaza-data.tsv
<html xmlns:v="urn:schemas-microsoft-com:vml"
xmlns:o="urn:schemas-microsoft-com:office:office"
xmlns:x="urn:schemas-microsoft-com:office:excel"
xmlns="http://www.w3.org/TR/REC-html40">

<head>

<meta name=ProgId content=Excel.Sheet>
<meta name=Generator content="Microsoft Excel 15">
<link id=Main-File rel=Main-File
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip.htm">
<link rel=File-List
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_filelist.xml">
<!--table
	{mso-displayed-decimal-separator:"\.";
	mso-displayed-thousand-separator:"\,";}
@page
	{margin:.75in .7in .75in .7in;
	mso-header-margin:.3in;
	mso-footer-margin:.3in;}
tr
	{mso-height-source:auto;}
col
	{mso-width-source:auto;}
br
	{mso-data-placement:same-cell;}
td
	{padding-top:1px;
	padding-right:1px;
	padding-left:1px;
	mso-ignore:padding;
	color:black;
	font-size:12.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"Aptos Narrow", sans-serif;
	mso-font-charset:0;
	mso-number-format:General;
	text-align:general;
	vertical-align:bottom;
	border:none;
	mso-background-source:auto;
	mso-pattern:auto;
	mso-protection:locked visible;
	white-space:nowrap;
	mso-rotate:0;}
.xl65
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl66
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:none;
	border-left:none;}
.xl67
	{font-weight:700;
	font-family:"Aptos Narrow";
	mso-generic-font-family:auto;
	mso-font-charset:0;
	border-top:none;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl68
	{font-weight:700;
	font-family:"Aptos Narrow";
	mso-generic-font-family:auto;
	mso-font-charset:0;
	border-top:none;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl69
	{text-align:center;
	border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl70
	{text-align:center;
	border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:none;
	border-left:.5pt solid windowtext;}
-->
</head>

<body link="#467886" vlink="#96607D">


Field | Description
-- | --
Rowid | Unique   identifier
Timestamp | Vehicle   timestamp
Anonymized   Vehicle Number | Car   ID
Vehicle   Type | Type
Number   of Axles | Axles



</body>

</html>
<br><br>
3️⃣ Payment-data.txt (Fixed Width)

<html xmlns:v="urn:schemas-microsoft-com:vml"
xmlns:o="urn:schemas-microsoft-com:office:office"
xmlns:x="urn:schemas-microsoft-com:office:excel"
xmlns="http://www.w3.org/TR/REC-html40">

<head>

<meta name=ProgId content=Excel.Sheet>
<meta name=Generator content="Microsoft Excel 15">
<link id=Main-File rel=Main-File
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip.htm">
<link rel=File-List
href="file:////Users/test/Library/Group%20Containers/UBF8T346G9.Office/TemporaryItems/msohtmlclip/clip_filelist.xml">

<!--table
	{mso-displayed-decimal-separator:"\.";
	mso-displayed-thousand-separator:"\,";}
@page
	{margin:.75in .7in .75in .7in;
	mso-header-margin:.3in;
	mso-footer-margin:.3in;}
tr
	{mso-height-source:auto;}
col
	{mso-width-source:auto;}
br
	{mso-data-placement:same-cell;}
td
	{padding-top:1px;
	padding-right:1px;
	padding-left:1px;
	mso-ignore:padding;
	color:black;
	font-size:12.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"Aptos Narrow", sans-serif;
	mso-font-charset:0;
	mso-number-format:General;
	text-align:general;
	vertical-align:bottom;
	border:none;
	mso-background-source:auto;
	mso-pattern:auto;
	mso-protection:locked visible;
	white-space:nowrap;
	mso-rotate:0;}
.xl65
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl66
	{border-top:.5pt solid windowtext;
	border-right:.5pt solid windowtext;
	border-bottom:none;
	border-left:none;}
.xl67
	{font-weight:700;
	font-family:"Aptos Narrow";
	mso-generic-font-family:auto;
	mso-font-charset:0;
	border-top:none;
	border-right:.5pt solid windowtext;
	border-bottom:.5pt solid windowtext;
	border-left:none;}
.xl68
	{font-weight:700;
	font-family:"Aptos Narrow";
	mso-generic-font-family:auto;
	mso-font-charset:0;
	border-top:none;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl69
	{text-align:center;
	border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:.5pt solid windowtext;
	border-left:.5pt solid windowtext;}
.xl70
	{text-align:center;
	border-top:.5pt solid windowtext;
	border-right:none;
	border-bottom:none;
	border-left:.5pt solid windowtext;}
-->
</head>

<body link="#467886" vlink="#96607D">


Field | Description
-- | --
Rowid | UID
Timestamp | Timestamp
Vehicle   Number | ID
Tollplaza   ID | ID
Tollplaza   Code | Code
Payment   Type | CASH /   PREPAID
Vehicle   Code | Category



</body>

</html>


## 🚀 ETL Architecture (Mermaid Diagram)
<img width="1648" height="678" alt="Image" src="https://github.com/user-attachments/assets/16c2d5b1-695b-49f9-856c-117f1e22fc80" />


