# Tapo Retriever

Automates retrieval of emails from GMail related to the Tapo P-110 Plug

The [Tapo P-110 Mini Smart Wi-Fi Socket, Power Monitoring](https://www.tapo.com/uk/product/smart-plug/tapo-p110/) mobile application has a facility to export data via email as an Excel (.xls) attachments. On Gmail I haev a rule that these emails are labelled 'Tapo'. The script is an ETL tools to access GMail, downlaod the attachments and extract and load the data to Postgres database.

Refer to [this page](https://developers.google.com/gmail/api/quickstart/python) on how to set credentials for GMail access.
