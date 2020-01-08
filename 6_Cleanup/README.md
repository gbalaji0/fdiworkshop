# Cleanup Instructions

**If you're running this inside of an Event Engine environment, you can stop reading.**

1. Navigate to your Cloud9 environment
1. Run the following commands to delete your resources:
    ```
    # Delete your bucket and all contents
    TeamRole:~/environment $ export bucketname=`aws s3 ls | grep simplebucket | cut -d" " -f3`
    TeamRole:~/environment $ aws s3 rb s3://$bucketname --force
    ```
1. Exit your Cloud9 environment
1. Select the environment you've been using
1. Click **Delete**
1. Follow the directions

**CONGRATS!** You're done!
