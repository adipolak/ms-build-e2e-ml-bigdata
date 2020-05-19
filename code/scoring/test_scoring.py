from azureml.core import Workspace
from azureml.core.authentication import AzureCliAuthentication
from azureml.core import Webservice
from azureml.core.model import InferenceConfig
from azureml.core.webservice import AciWebservice
from azureml.exceptions import WebserviceException
import json
import numpy


# load Azure ML workspace
azureml_workspace = Workspace.from_config(
    auth=AzureCliAuthentication()
)

# the name of the scoring service
service_name = 'comments-clf-westeurope'

service = Webservice(
    azureml_workspace,
    service_name
)

input_payload = json.dumps({
    "data": "this is some text about gaming"
})

request = json.loads(input_payload)["data"]

output = service.run(input_payload)

print(output)