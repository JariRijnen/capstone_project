class SparkSteps:
    """Spark steps to execute on the EMR cluster."""
    SPARK_STEPS = [
        {
            "Name": "Move raw data from S3 to HDFS",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    "--src=s3://{{ params.BUCKET_NAME }}/{{ params.wildfire_s3_data }}",
                    "--dest=/wildfires",
                ],
            },
        },
        {
            "Name": "Pre-Process wildfire data",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                ],
            },
        },
        {
            "Name": "Move clean data from HDFS to S3",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    "--src=/output",
                    "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
                ],
            },
        },
    ]
