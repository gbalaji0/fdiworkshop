# Perform Complex Analytics and Periodic Reporting using Amazon Redshift
**Optional** - to skip [click here to move on to next step](#next-step)

**Time to complete:** 30-45 minutes.

### Step 1: Setup Amazon Redshift

1. Navigate to Amazon Redshift [in the console](https://redshift.aws.amazon.com/)

1. Click on Quick Launch Cluster
  ![Quick Launch Cluster](assets/RedshiftQuickLaunch.png)

1. Choose “Master user password” (remember the password)

1. Choose “Team Role” for “Available IAM roles” and click on “Launch Cluster”
  ![Team Role selection](assets/RedshiftLaunchClusterRole.png)

1. In Redshift dashboard click on “Clusters” to check status of cluster creation
  ![Cluster creation](assets/RedshiftClusterComplete.png)

### Step 2: Setup VPC Endpoint for S3

1. Navigate to VPC services [in the console](https://quicksight.aws.amazon.com/)

1. Click on **Endpoints** and select **create endpoint**
  ![Create endpoint](assets/VPCCreateEndpoint.png)

1. Choose “AWS Services” in Service Category, and select S3.
  ![Select S3 endpoint](assets/VPCEndpointChooseS3.png)

1. Choose the VPC, and all the associated subnets.
  ![Choose VPC and subnets](assets/VPCEndpointRouteTable.png)

1. In Policy, select “Full Access”, and hit create endpoint
  ![S3 access policy](assets/VPCEndpointPolicyCreateEndpoint.png)

You have now successfully setup VPC endpoint for S3

### Step 3: Enable Amazon Redshift access from Amazon QuickSight

  1. Navigate back to the VPC landing page [in the console](https://console.aws.amazon.com/vpc/)

  1. Click on **Security Groups**

  1. Select default security group, select edit inbound rules, and click on **Edit rules**
    ![Security Groups inbound rules](assets/VPCSGInboundRules.png)

  1. Add Inbound Rules for Redshift

  1. For **Type** select "Redshift"

  1. For **Source**, specify the QuickSight CIDR range. The source address for QuickSight can be got from [QuickSight Regions, and IP Ranges](https://docs.aws.amazon.com/quicksight/latest/user/regions.html)

  <details>
  <summary><strong>Expand if you want detailed directions</strong></summary><p>

    1. Navigate to [QuickSight Regions, and IP Ranges](https://docs.aws.amazon.com/quicksight/latest/user/regions.html)
    1. Locate the section corresponding to the region your instance is running (E.g., _us_east_1_)
    1. Find the **IP address range** and copy the corresponding value (E.g., _52.23.63.224/27_)

    This is the QuickSight CIDR range needed to be configured in the Security Group setting.

  </p></details><br/>

  7. Click on **Save rules** to update Security Group rules to allow QuickSight access the Redshift port.
  ![Security Groups inbound rules](assets/VPCAddInboundRulesForQS.png)

### Step 4: In AWS Glue, check connection to Redshift
  1. Navigate to AWS Glue [in the console](https://console.aws.amazon.com/glue)

  1. Click on **Connections**, then select **Add Connection**
  ![Redshift AddConnection](assets/GlueAddRSConnection.png)

  1. Specify a **Connection name** and choose **ConnectionType** as “Amazon Redshift” and click on **Next** button
  ![Redshift Connection Details](assets/GlueRSConnectionProp.png)

  1. Choose the Redshift cluster. Specify **Database name** as “dev”, **Username**: “awsuser”, and for **Password** use the password chosen during Redshift Cluster creation.
  ![Redshift Cluster Creation](assets/GlueRSCluster.png)

  1. Review connection details and click on **Finish** button
  ![Review Redshift Connection details](assets/GlueRSReview.png)

  1. Now select **Connections** on the left tab, then choose the connection name and hit **Test Connection**. You should see “connected successfully” message. If not, recheck the configuration steps.
  ![Redshift Test Connection](assets/GlueRSTestConnection.png)

### Step 5: Load data onto Amazon Redshift using AWS Glue

  1. In AWS Glue, Select Jobs in ETL section. Click on “Add Job”
  ![Glue Add Job](assets/GlueAddJob.png)

  1. Configure Job - Provide Name such as “BBGRedshift”, Choose “Team Role”
  ![Glue Redshift Job config](assets/GlueRSJobConfig.png)

  1. For the “S3 path where the script is stored”, Point to _bbgGlueRedshift.py_ script in S3
  ![Glue Redshift script location in S3](assets/GlueRSScriptLocationinS3.png)

  1. Select the Redshift connection, that was tested before, Save and Edit script
  ![Glue Job Add connection](assets/GlueJobAddConnection.png)

  1. Run the ETL job
  The job takes few minutes to run. Once the job completes, you will see the Run status, reflect as “Succeeded”

### Step 6: Query data in Amazon Redshift
  1. Verify Table in _Query Editor_ within Amazon Redshift
  Click on _Query Editor_ and select _public_ in Schema and make sure table, **bbg** appears in the table list.

  1. Verify the content by running sample query such as the follows

  `select * from public.bbg limit 10`

   ##### Step 6a: Redshift Spectrum
   1. Create Glue crawler for _industry_ data
      1. In AWS Glue, select the Crawlers tab on the left pane and click on “Add crawler”
      ![Glue Add Crawler](assets/IndustryAddCrawler.png)

      1. Crawler Info, give a name such as “industry”
      ![Crawler name](assets/CrawlerInfo.png)

      1.	In “crawler source type”, choose “Data Stores”
      ![Crawler source type](assets/AddCrawler_SourceType.png)

      1.	In “Data Store”, choose the corresponding S3 path for industry file
      ![Crawler S3 Datasource](assets/AddCrawler_ChooseS3.png)

      1.  For IAM Role, choose “Team Role”
      ![Crawler Choose IAM Role](assets/AddCrawler_ChooseIAMRole.png)

      1.	Next in Schedule/Frequency, use “Run on-demand”
      ![Crawler Schedule](assets/AddCrawler_Schedule.png)

      1.	In Output, choose the “spectrumdb”
      ![Crawler output to spectrumdb](assets/AddCrawler_Output.png)

      1.	Review all the selections
      ![Crawler Review](assets/AddCrawler_Review.png)

      1.	Run the crawler, by clicking on "Run it now?"
      ![Crawler Run on Demand](assets/AddCrawler_RunOnDemand.png)

      1.	In the Tables tab, verify that the catalog has been created.
      ![Verify Industry table catalog](assets/SpectrumIndustryTableCatalogCreated.png)

      1.	Inspect the catalog by double clicking on the name.
      ![Inspect Industry table in catalog](assets/SpectrumIndustryTableInCatalog.png)

   1.	Link external schema to glue data catalog
      In Redshift Query Editor, run the following commands.
   ```
      create external schema spectrum_schema from data catalog
      database 'spectrumdb'
      iam_role  '<TeamRoleARN>'
      create external database if not exists;
   ```
      <details>
      <summary>Expand if you want detailed directions to get the <strong>TeamRoleARN</strong></summary><p>

      1. Navigate to IAM [in the console](https://console.aws.amazon.com/iam) and click on **Roles** in the left panel, choose **TeamRole**, copy the **Role ARN** in the summary section, and copy the TeamRoleARN
      ![IAM TeamRoleARN](assets/IAMTeamRoleARN.png)

      </p></details>

   1.	Illustrate Redshift Spectrum Join
    In Redshift in Query Editor, run following Query. It joins “bbg” data in Redshift with “industry” data in S3.
   ```
    select b.sector, count(a.ticker) as numInSector, sum(a.px_volume) as totVolbySector
    from bbg a, spectrum_schema.industry b
    where a.last_update_dt_exch_tz=20180809 and a.exch_code='US'
    and b.symbol = a.ticker
    group by b.sector
    order by totVolbySector desc
   ```

### Step 7: Visualize data in Amazon QuickSight

  1. To visualize data in **Amazon QuickSight**, follow the instructions similar to [Amazon QuickSight section](../3_AmazonQuickSight), but use **Amazon Redshift** as a data source and queries from above.

## Next step:

We're ready to gain insights using Machine Learning with [Amazon SageMaker](../5_AmazonSagemaker).
