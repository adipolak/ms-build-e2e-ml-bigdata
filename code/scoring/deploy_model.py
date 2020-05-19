import os
import argparse
from azureml.core.model import InferenceConfig
from azureml.core.environment import Environment
from azureml.core.webservice import AciWebservice
from azureml.core.model import Model
from azureml.core import Workspace
from azureml.core.authentication import AzureCliAuthentication


# Parse Definition Arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "-n",
    "--name",
    dest='name',
    type=str,
    default='redditcomments-gaming-model',
    help='The model name'
)
parser.add_argument(
    "-v",
    "--version",
    dest='version',
    type=int,
    default=4,
    help='The model version'
)
parser.add_argument(
    "-o",
    "--output",
    dest='output_dir',
    type=str,
    help='output folder'
)
parser.add_argument(
    "-s",
    "--service_name",
    dest='service_name',
    type=str,
    default="comments-clf-eastus",
    help='the web service name'
)
args = parser.parse_args()

print("Model name: ", args.name)
print("Model version: ", args.version)

# Load the AML Workspace and Model
ws = Workspace.from_config(
    auth=AzureCliAuthentication()
)

model = Model(
    workspace=ws,
    name=args.name,
    version=args.version
)

# Configure Scoring App Environment
scoringenv = Environment.from_conda_specification(
    name="scoringenv",
    file_path=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'scoring_environment.yml'
    )
)

inference_config = InferenceConfig(
    entry_script=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'score.py'
    ),
    environment=scoringenv
)

# Configure Deployment Compute
compute_config = AciWebservice.deploy_configuration(
    cpu_cores=1,
    memory_gb=1,
    location="eastus", #eastus # australiaeast
    tags={
        'purpose': 'build2020'
    },
    description='Deployment for Build 2020'
)

# Run the deployment
deployment = Model.deploy(
    workspace=ws,
    name=args.service_name,
    models=[model],
    inference_config=inference_config,
    deployment_config=compute_config
)

# Wait for completion
deployment.wait_for_deployment(True)

if deployment.state != 'Healthy':
    logs = deployment.get_logs()
    raise Exception('Service Deployment Failed {}'.format(logs))

else:
    print("Deployment was succesful")
    print("Scoring URI is {}".format(deployment.scoring_uri))