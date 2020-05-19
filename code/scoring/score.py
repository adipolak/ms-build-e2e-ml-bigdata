import os
import pickle
import json
import numpy
import joblib


def init():
    global model
    
    if os.getenv('AZUREML_MODEL_DIR') is not None:
        # load cloud path
        model_path = os.path.join(
            os.getenv('AZUREML_MODEL_DIR'),
            'model-naive-bayes-classifier/model.pkl'
        )
    else:
        # load local for debug
        model_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '../../outputs/model-naive-bayes-classifier',
        'model.pkl'
    )

    # deserialize the model file back into a sklearn model
    model = joblib.load(model_path)


def run(raw_data):
    try:
        # take some raw data
        data = json.loads(raw_data)['data']

        data = numpy.array([data])

        # Score using sklearn classification pipeline
        result = model.predict(data)

        # you can return any data type as long as it is JSON-serializable
        return result.tolist()

    except Exception as e:
        error = str(e)
        return error
