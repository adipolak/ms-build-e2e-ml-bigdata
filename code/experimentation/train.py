import argparse
import mlflow
import mlflow.sklearn
from azureml.core import Dataset, Run
from sklearn.metrics import (
    precision_score, recall_score, accuracy_score, classification_report)
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.pipeline import Pipeline
from sklearn.naive_bayes import MultinomialNB


# Parse script arguments like hyperparameters
parser = argparse.ArgumentParser()
parser.add_argument(
    '--alpha',
    type=float,
    dest='alpha',
    default=1.0,
    help='alpha'
)
args = parser.parse_args()

# Retrieve the AML Context for dataset access and tracking
run = Run.get_context()

# Run the experiment
with mlflow.start_run():

    # Retrieve the dataset that was passed at job submission
    dataset = run.input_datasets['comments']
    dataframe = dataset.to_pandas_dataframe()

    # Split the data
    train, test = train_test_split(dataframe)
    
    # Construct a training sklearn pipeline with three stages
    # 1. Tokenize text - build vocabulary and count word occurances
    # 2. Transform count matrix to normalized TF-IDF
    # 3. Train a Naive Bayes classifier
    text_clf = Pipeline([
        ('vect', CountVectorizer()),
        ('tfidf', TfidfTransformer()),
        ('clf', MultinomialNB(
            alpha=args.alpha
        )),
    ])

    # Train a model
    text_clf.fit(train.text, train.meta)

    # Score against the test test
    predicted_y = text_clf.predict(test.text)

    # Evaluate based on test set and log with MLFlow
    mlflow.log_metric("precision", precision_score(predicted_y, test.meta, average='weighted'))
    mlflow.log_metric("recall", recall_score(predicted_y, test.meta, average='weighted'))
    mlflow.log_metric("accuracy", accuracy_score(predicted_y, test.meta))

    # Save the model in MLFlow packaging format
    mlflow.sklearn.log_model(
        sk_model=text_clf,
        artifact_path="model-naive-bayes-classifier"
    )

    # Register the model and link the input dataset
    run.register_model(
        model_name='redditcomments-gaming-model',
        model_path="model-naive-bayes-classifier",
        datasets=[
            ('training data', dataset)
        ]
    )