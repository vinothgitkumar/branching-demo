# Fivetran

## Facebook Ads

### Description

Facebook ads is a native connector from Fivetran which provides us with Facebook marketing campaign data.

More details on the connector can be found here - https://fivetran.com/docs/applications/facebook-ads#facebookads

### Data we receive

The data populated by this connector will act as a replacement for the data we get in Luigi from facebook_marketing 
pipeline populating the table - `hive.facebook.marketing_campaigns`

## About This Dag

The goal of this dag is to wait for the bronze data to be available in S3 and trigger the silver notebook once the data
is received from Fivetran.

To achieve the said goal this DAG makes use of tardis sensor which is continuously listening to the status
`Data Staged`. Once this status is updated in tardis with the help of streaming notebook, the sensor task gets completed
and the silver notebook for the corresponding connector gets triggered.

Task Flow:-

`Start --> Set Databricks Connection --> Check Tardis Status --> Trigger DataBricks Notebook --> End`

 
Note : The above flow starting from `Check Tardis Status` gets executed for all the connectors in a loop where each 
connector represents a marketing account.
