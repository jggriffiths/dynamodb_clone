# Problem
You have a need to clone an AWS DynamoDB table from one table or account to another.  You are using OnDemand provisioning, which does not allow backups from Data Pipeline, which requires provisioned tables.

# Solution
There's a ridiculous EMR/Hive solution suggested by AWS support.

This is a low-tech, but quick solution to the problem.  Read from one table, slam it into another.

# Usage
Uses 'boto3' and IAM user keys by default.  It asks for source and destination table, key, secret, and region.  Allows for deleting destination data before copying.

# Limitations
Reads every object.  Writes every object.  Good luck**

**Not responsible for any read/write charges, problems, etc.

