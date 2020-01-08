# Visualize data using Amazon QuickSight

**Time to complete:** 15-30 minutes.

### Step 1: Signup for Amazon QuickSight settings

1. Navigate to Amazon QuickSight [in the console](https://quicksight.aws.amazon.com/)

1. Signup for QuickSight

  ![Record count](assets/QSSignupScreen.png)

1. Select “Enterprise” edition

  ![Record count](assets/QSSignupEdition.png)

1. Provide Amazon QuickSight account name and a notification email address

1. Choose S3 location, and pick the **&lt;mod-...-simplebucket-...&gt;** bucket

  ![Record count](assets/QSS3Selection.png)


### Step 2: Amazon QuickSight dataset configuration

1. Navigate back to the QuickSight landing page [in the console](https://quicksight.aws.amazon.com/)

1. Click on **Manage data** button on the top right corner

  ![Record count](assets/QSManageDataset.png)

1. Click on **New data set**

  ![Record count](assets/QSNewDataset.png)

1. Click **Athena** and in **Data source name** type “bbg”

  ![Record count](assets/QSAthenaConnection.png)

1. Click on **Validate connection** to verify

1. Click on **Create data source** , choose **marketdata** database and **bbg** table

1. Select **Custom SQL** and use the following query
  ```
  select * from "marketdata"."bbg" where ticker is not null
  ```

    ![Record count](assets/QSCustomSQL.png)

1. Click on **Confirm Query**

1. Select **Directly query your data** option and Click on **Visualize**

  ![Record count](assets/QSVisualizeSelection.png)

### Step 3: Visualize raw data in Amazon QuickSight

1. Continue on the Visualization from previous step
Note: If navigated out, go back to QuickSight dashboard and choose "New custom SQL analysis" that was created recently.

1. Select **Line Chart** from the **Visual types** in the bottom left corner

  ![Record count](assets/QSSelectLineChart.png)

1. Choose _settle_dt_ for **X axis**, _px_volume_ for **value**, and choose _security_typ_ for **color**

  ![Record count](assets/QSLineChart.png)
Note: Click on down arrow next to *settle_dt* in the chart and change format without the commas

1. Similarly try **Bar Charts**, **Pivot Tables** to visualize the data.

  ![Record count](assets/QSLineBarPivotTable.png)

## Next step:

We're ready to do complex analytics and periodic reporting using [Amazon Redshift](../4_AmazonRedshift).
