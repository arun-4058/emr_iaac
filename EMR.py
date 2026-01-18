import json
from enum import Enum


class Instance:
    class Role(Enum):
        MASTER = "MASTER"
        CORE = "CORE"
        TASK = "TASK"

    class Market(Enum):
        ON_DEMAND = "ON_DEMAND"
        SPOT = "SPOT"

    class Type(Enum):
        R8G_XLARGE = "r8g.xlarge"
        R8G_2XLARGE = "r8g.2xlarge"
        R8G_4XLARGE = "r8g.4xlarge"
        R8G_8XLARGE = "r8g.8xlarge"
        R8G_12XLARGE = "r8g.12xlarge"
        R8G_16XLARGE = "r8g.16xlarge"
        R8G_24XLARGE = "r8g.24xlarge"
        R8G_48XLARGE = "r8g.48xlarge"
        R7G_XLARGE = "r7g.xlarge"
        R7G_2XLARGE = "r7g.2xlarge"
        R7G_4XLARGE = "r7g.4xlarge"
        R7G_8XLARGE = "r7g.8xlarge"
        R7G_12XLARGE = "r7g.12xlarge"
        R7G_16XLARGE = "r7g.16xlarge"
        R7GD_XLARGE = "r7gd.xlarge"
        R7GD_2XLARGE = "r7gd.2xlarge"
        R7GD_4XLARGE = "r7gd.4xlarge"
        R7GD_8XLARGE = "r7gd.8xlarge"
        R7GD_12XLARGE = "r7gd.12xlarge"
        R7GD_16XLARGE = "r7gd.16xlarge"
        R6G_XLARGE = "r6g.xlarge"
        R6G_2XLARGE = "r6g.2xlarge"
        R6G_4XLARGE = "r6g.4xlarge"
        R6G_8XLARGE = "r6g.8xlarge"
        R6G_12XLARGE = "r6g.12xlarge"
        R6G_16XLARGE = "r6g.16xlarge"
        DEFAULT = "r8g.xlarge"

    def __init__(self, role: Role, instance_type: Type, count: int, name: str, market: Market, ebs_config: dict = None):
        """Initializes an Instance and validate its Configuration"""
        self._validate_instance(market, role, count)
        self.config = {
            "Name": name,
            "Market": market.value,
            "InstanceRole": role.value,
            "InstanceType": instance_type.value,
            "InstanceCount": count,
            "EbsConfiguration": ebs_config or EBS().config
        }

    @staticmethod
    def _validate_instance(market: Market, role: Role, count: int):
        """Validate Instance Configuration"""
        if role.value.upper() == "MASTER":
            if market.value.upper() != "ON_DEMAND":
                raise ValueError("Primary node instance should always be ON_DEMAND")
            if count != 1:
                raise ValueError("Primary node count should be 1")
        if role.value.upper() == "CORE":
            if market.value.upper() != "ON_DEMAND":
                raise ValueError("Core node instance should always be ON_DEMAND")


class EBS:
    GP2 = "gp2"
    GP3 = "gp3"
    DEFAULT = "gp3"

    def __init__(self, volume_type: str = "gp3", volume_size: int = 15, volume_per_instance: int = 1):
        """Initializes EBS"""
        self.config = {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "VolumeType": volume_type if volume_type else self.DEFAULT,
                        "SizeInGB": volume_size if volume_size and volume_size >= 15 else 15
                    },
                    "VolumesPerInstance": volume_per_instance if volume_per_instance and volume_per_instance >= 1 else 1
                }
            ],
            "EbsOptimized": True
        }


class InstanceFleets:

    def __init__(self, instance_fleet_config_path: str, airflow_env: str = "local", ebs_config: dict = None):
        absolute_config_path = '/airflow-efs/' + airflow_env + '/git/latest/dags/' + instance_fleet_config_path if airflow_env != "local" else instance_fleet_config_path
        with open(absolute_config_path) as instance_fleet_config:
            instance_fleet_json = json.load(instance_fleet_config)
        for fleet_type in instance_fleet_json:
            for instance_type_config in fleet_type['InstanceTypeConfigs']:
                instance_type_config['EbsConfiguration'] = ebs_config if ebs_config else EBS().config

        self.config = instance_fleet_json


class EMR:
    DEFAULT_INSTANCE_GROUPS = [
        {
            "Name": "MasterInstanceGroup",
            "Market": "ON_DEMAND",
            "InstanceRole": "MASTER",
            "InstanceType": "r8g.xlarge",
            "InstanceCount": 1,
            "EbsConfiguration": EBS().config
        },
        {
            "Name": "CoreInstanceGroup",
            "Market": "ON_DEMAND",
            "InstanceRole": "CORE",
            "InstanceType": "r8g.xlarge",
            "InstanceCount": 1,
            "EbsConfiguration": EBS().config
        }
    ]

    DEFAULT_TAGS = [
        {"Key": "AlwaysOn", "Value": "false"},
        {"Key": "TtlInHr", "Value": "24"},
    ]

    def _dedupe_tags(self, default_tags: list[dict], user_tags: list[dict], env: str) -> list[dict]:
        """
        De-dupes the tags. If there is any duplicate tag provided by the user,
        then priority will be given to user tag, default tag will be updated by the user tag.
        """
        dt = {d['Key']: d['Value'] for d in default_tags}
        if user_tags:
            ut = {d['Key']: d['Value'] for d in user_tags}
            for k, v in ut.items():
                dt[k] = v
        dt['Environment'] = env
        final_tags = [{"Key": key, "Value": value} for key, value in dt.items()]
        return final_tags

    def __init__(self,
                 script_args: str = "",
                 cluster_name: str = "TestCluster",
                 instance_groups: list[dict] = None,
                 instance_fleets: list[dict] = None,
                 release_label: str = 'emr-7.5.0',
                 log_uri: str = None,
                 environment: str = "dev",
                 tags: list[dict] = None,
                 bootstrap_actions: list[dict] = None):

        default_bootstrap_actions = [
            {
                "Name": "runScript",
                "ScriptBootstrapAction": {
                    "Path": "s3://useast1-test-bucket/bootstrap/script.sh",
                    "Args": [script_args]
                }
            }
        ]

        self.config = {
            "Name": f"TestCluster_{environment}",
            "LogUri": log_uri or 's3://aws-logs-2387423742-us-east-1/elasticmapreduce/test-cluster/',
            "ReleaseLabel": release_label,
            "Instances": {
                "InstanceGroups" if instance_groups else "InstanceFleets" : instance_groups if instance_groups else instance_fleets,
                "Ec2KeyName": "DEV-KEY",
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2SubnetId" if instance_groups else "Ec2SubnetIds" : "subnet-asdc23rdca34" if instance_groups else ["subnet-asdc23rdca34", "subnet-asdc23rdca34", "subnet-asdc23rdca34"],
                "EmrManagedMasterSecurityGroup": "sg-asdjfn43r243d234",
                "EmrManagedSlaveSecurityGroup": "sg-asdjfn43r243d234",
                "ServiceAccessSecurityGroup": "sg-asdjfn43r243d234"
            },
            "ServiceRole": "emr-default-role",
            "JobFlowRole": "emr-ec2-role",
            "VisibleToAllUsers": True,
            "Tags": self._dedupe_tags(self.DEFAULT_TAGS, tags, environment),
            "Applications": [{"Name": "Hadoop"}, {"Name": "Hive"}, {"Name": "Spark"}],
            "Configurations": [
                {
                    "Classification": "spark",
                    "Properties": {"maximizeResourceAllocation": "true"}
                },
                {
                    "Classification": "yarn-site",
                    "Properties": {
                        "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
                    }
                },
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {"Classification": "export", "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}}]
                }
            ],
            "BootstrapActions": default_bootstrap_actions + (bootstrap_actions if bootstrap_actions else []),
            "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
            "EbsRootVolumeSize": 15
        }

    @classmethod
    def create_cluster_config(cls, cluster_name: str, cluster_config_args: dict, tags: list[dict] = None):
        cluster_config_args['cluster_name'] = cluster_name
        if tags:
            cluster_config_args['tags'] = tags
        return cls(**cluster_config_args)


# Example Usage
if __name__ == '__main__':
    MASTER = Instance.Role.MASTER
    CORE = Instance.Role.CORE
    TASK = Instance.Role.TASK
    ON_DEMAND = Instance.Market.ON_DEMAND
    SPOT = Instance.Market.SPOT
    InstanceType = Instance.Type
    environment = "stage"
    script_args = "script_args"
    test_cluster_1 = {
        "instance_groups": [
            Instance(MASTER, InstanceType.R6G_XLARGE, 1, "MasterInstanceGroup", ON_DEMAND, EBS().config).config,
            Instance(CORE, InstanceType.R8G_8XLARGE, 5, "CoreInstanceGroup", ON_DEMAND,
                     EBS(volume_size=100).config).config,
        ],
        "environment": environment,
        "script_args": script_args
    }

    test_cluster_2 = {
        "instance_groups": [
            Instance(MASTER, InstanceType.R6G_XLARGE, 1, "MasterInstanceGroup", ON_DEMAND, EBS().config).config,
            Instance(CORE, InstanceType.R8G_8XLARGE, 5, "CoreInstanceGroup", ON_DEMAND,
                     EBS(volume_size=100).config).config,
        ],
        "environment": environment,
        "tags": [{"Key": "ClusterType", "Value": "IncrementalAggregator"}],
        "script_args": script_args
    }

    cluster_name = "test_cluster_1"
    tags = [{"Key": "ClusterType", "Value": "IncrementalAggregator"}]
    test_cluster_1_config = EMR().create_cluster_config(cluster_name, test_cluster_1, tags=tags).config
    test_cluster_2_config = EMR().create_cluster_config("test_cluster_2", test_cluster_2).config

    print(test_cluster_1_config)
    print(test_cluster_2_config)

    test_cluster_3 = {
        "instance_groups": [
            Instance(MASTER, InstanceType.R8G_4XLARGE, 1, "MasterInstanceGroup", ON_DEMAND,
                     EBS(volume_size=64).config).config,
            Instance(CORE, InstanceType.R8G_12XLARGE, 5, "CoreInstanceGroup", ON_DEMAND,
                     EBS(volume_size=256).config).config,
            Instance(TASK, InstanceType.R8G_12XLARGE, 20, "MasterInstanceGroup", ON_DEMAND,
                     EBS(volume_size=256).config).config,
        ],
        "environment": environment,
        "script_args": script_args,
        "tags": [
            {"Key": "AlwaysOn", "Value": "false"},
            {"Key": "ApplicationName", "Value": "Aggregator1"},
            {"Key": "CreatedBy", "Value": "de@gamil.com"}
        ]
    }

    test_cluster_with_fleet = {
        "instance_fleets": InstanceFleets("instance_fleets/adhoc_cluster_fleet.json").config,
        "environment": environment,
        "script_args": script_args,
        "tags": [
            {"Key": "AlwaysOn", "Value": "false"},
            {"Key": "ApplicationName", "Value": "FleetCluster"},
            {"Key": "CreatedBy", "Value": "de@gmail.com"}
        ]
    }

    test_cluster_with_fleet_config = EMR().create_cluster_config("advance_audience", test_cluster_with_fleet).config
    print(test_cluster_with_fleet_config)
