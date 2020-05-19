# Helper file to submit an experiment run
import os
from azureml.core import Workspace, Experiment, Dataset
from azureml.core.model import Model
from azureml.train.estimator import Estimator
from azureml.core.authentication import AzureCliAuthentication
from azureml.data.data_reference import DataReference


# load Azure ML workspace
azureml_workspace = Workspace.from_config(
    auth=AzureCliAuthentication()
)

# Retrieve a pointer to the dataset versions
redditcomments_gaming = Dataset.get_by_name(
    azureml_workspace,
    name='redditcomments',
    version='latest'
)

redditcomments = Dataset.get_by_name(
    azureml_workspace,
    name='redditcomments_gaming',
    version='latest'
)

# Configure the training run
est = Estimator(
    entry_script='train.py',
    script_params = {  
        '--alpha': 1.0
    },
    source_directory=os.path.dirname(os.path.realpath(__file__)),
    compute_target='ml-e2e',
    inputs=[
        redditcomments_gaming.as_named_input('comments')
    ],
    pip_packages=[
        "azureml-sdk",
        "azureml-mlflow",
        "matplotlib",
        "scipy",
        "sklearn",
        "azure-cli",
        "pandas",
        "numpy"
    ]
)

# Submit an experiment run
experiment = Experiment(
    azureml_workspace,
    "redditcomments_gaming_experiment"
)

run = experiment.submit(est)

run.wait_for_completion(show_output=True)
