import os
from dotenv import load_dotenv

load_dotenv()
s3_logs_folder = os.getenv('s3_logs_folder')


class EmrClusterConfig:
    JOB_FLOW_OVERRIDES = {
        "Name": "Wildfire cluster",
        "ReleaseLabel": "emr-6.5.0",
        "LogUri": s3_logs_folder,
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
        "Configurations": [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                    }
                ],
            }
        ],
        "Instances": {
            "Ec2KeyName": "emr-key",
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m4.xlarge",
                    "InstanceCount": 1,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    }
