# GPU based, batch and streaming predictions on Dataflow

## Introduction

Dataflow allows you to run a process in a custom Docker container, optionally with a GPU.

These 2 features bring new possibilities for people who want to run batch and streaming
predictions on Dataflow. Thanks to Dockerized environment, you can define any
prediction environment you need, and make predictions more efficient using GPU.

BigFlow 1.3 comes with utilities that make your life easier when it comes to writing Dockerized
processes and writing prediction logic.

This recipe will show you how to set up a PyTorch prediction process, but the dockerization allows you
to define an environment for any framework. We assume that you are already familiar with the BigFlow API.

We are going to create a batch prediction process for the Fashion MNIST model, using Dataflow. You can download
the model [here](fashion.pth). We won't focus on the utility or performance side of the project, but more on the API that
BigFlow provides, which makes creating prediction processes easier.

## Docker image and packages

To run a prediction process on GPU, you need a Docker file with the CUDA drivers. The image below has all the dependencies
for running a PyTorch prediction process, and also it's compatible with BigFlow. Use that image as the image for your
BigFlow project.

```dockerfile
# Docker base image, includes os-level tools (shell, utilities, services), python interpreter, C-libraries, etc.

# use it if you want ot use custom Docker image on Dataflow workers
FROM apache/beam_python3.7_sdk:2.27.0


# Cherry-pick apache beam binaries from offical image to our custom.
# Use this only in case when you unable to use original image (FROM apache/beam...)
# COPY --from=apache/beam_python3.6_sdk:2.26.0 /opt/apache/beam /opt/apache/beam


# Install CUDA drivers to your docker
# Note - this will increase size of your image dramatically, which has influence
# on building and deployment timings as well. Do not install CUDA unless you are using GPU.
#
RUN echo "Installing CUDA ..." \
  && wget https://developer.nvidia.com/compute/cuda/10.0/Prod/local_installers/cuda_10.0.130_410.48_linux \
  && sh cuda_10.0.130_410.48_linux --toolkit --silent --override \
  && rm cuda_10.0.130_410.48_linux \
  && echo "CUDA was installed!"
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/nvidia/lib64:/usr/local/cuda/lib64


# Working directory .
WORKDIR /app


# Install apt packages.  Add custom libraries, dev-packages, c++ compiler etc.
RUN apt-get update && apt-get install -y \
    # libc-dev \
    && rm -rf /var/lib/apt/lists/*


# Preinstall python packages (docker layer caching).
# Manually install package if you can't put it into your `requirements.in` for some reason.
COPY ./resources/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install my-awesome-package==1.2.3


# Install bigflow project 'whl' package.
COPY ./dist dist
RUN pip install dist/*.whl
```

## Writing prediction process

First, create a new BigFlow project called `fashionmnist`, using scaffolding tool and remove automatically-generated
workflows.

Next, define the model in `fashionmist.model` module:

```python
from torch import nn

classes = [
    "T-shirt/top",
    "Trouser",
    "Pullover",
    "Dress",
    "Coat",
    "Sandal",
    "Shirt",
    "Sneaker",
    "Bag",
    "Ankle boot",
]

device = 'cuda'


class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28*28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10),
            nn.ReLU(),
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits
```

Now, let us define the processing logic and workflow:

```python
import logging
import uuid

import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import bigflow.dataflow
import bigflow.dataflow.io as bf_io
import bigflow.dataflow.ml as bf_ml

import fashionmist.model as fmnist

logger = logging.getLogger(__name__)

config = bigflow.Config(
    name='dev',
    properties=dict(
        project='<PUT YOUR PROJECT ID HERE>',
        staging_location='gs://<PUT YOUR PROJECT ID HERE>/beam_runner/staging',
        temp_location='gs://<PUT YOUR PROJECT ID HERE>/beam_runner/temp',
        region='europe-west1',
        runner='DataflowRunner',
        machine_type='n1-standard-4',
        num_workers=1,
        disk_size_gb=50,
        model_path="gs://<PUT YOUR PROJECT ID HERE>/fashion.pth",
        experiments=["worker_accelerator=type:nvidia-tesla-k80;count:1;install-nvidia-driver"],
    ),
).resolve()


class PytorchClassifierModel(bf_ml.BaseModel):

    def load_model(self, path, **kwargs):
        model = fmnist.NeuralNetwork()
        with bf_io.download_file_to_localfs(self.model_path) as m_path:
            model.load_state_dict(fmnist.torch.load(m_path))
            model = model.to(fmnist.device)
            model.eval()
        return model

    def predict(self, x: pd.DataFrame):
        X = x['value'].iloc[0]
        X = X.to(fmnist.device)
        model = self.ensure_model()
        with fmnist.torch.no_grad():
            pred = model(X)
            predicted = fmnist.classes[pred[0].argmax(0)]
            logger.info(f'Prediction sample: "{predicted}"')


class LoadBatchesDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        test_dataset = fmnist.datasets.FashionMNIST(
            root='data',
            train=False,
            download=True,
            transform=fmnist.ToTensor(),
        )
        test_dataloader = fmnist.DataLoader(test_dataset, batch_size=100)
        for batch, (X, y) in enumerate(test_dataloader):
            yield pd.DataFrame([
                {
                    'batch_id': str(uuid.uuid1()),
                    'value': X
                }
            ])


def run_predictions(
        pipeline: beam.Pipeline,
        context: bigflow.JobContext,
        _entry_point_arguments,  # unused
):
    model = PytorchClassifierModel(model_path=config['model_path'])

    return (pipeline
            | "InitialImpulse" >> beam.Create(['impulse'])
            | "LoadBatches" >> beam.ParDo(LoadBatchesDoFn())
            | "ReshuffleWorkload" >> beam.Reshuffle()
            | "Predict" >> bf_ml.ApplyModel(model=model, key_column='batch_id')
            )


workflow = bigflow.Workflow(
    workflow_id="fmnist",
    definition=[
        bigflow.dataflow.BeamJob(
            id='prediction',
            entry_point=run_predictions,
            use_docker_image=True,
            pipeline_options=PipelineOptions.from_dictionary(config),
        ),
    ],
)
```

Take a closer look at `BeamJob` class usage. Because the `use_docker_image` flag is set to `True`, the job object
uses the Docker image from your project as the execution environment for the job. You can also specify some other Docker 
image, by setting the `use_docker_image` as a string that specifies the image address.

The second thing is `bf_ml.BaseModel` class paired with `bf_ml.ApplyModel` transform. These 2 classes form the BigFlow 
API for predictions. The `BaseModel` class has 2 methods you should override. 

The `load_model` method should load and prepare the model before prediction. It takes the single `path` argument, it can be
pretty much anything, for example, the path to a GCS object. That method should return a ready-to-use model.

The `predict` method takes the single `x: pd.Dataframe` argument, which represents a batch of records for prediction.
To get the model defined in the `load_model` method, you should use the `ensure_model` method. The `x: pd.Dataframe` 
the argument should be a batch of records for prediction. You can return the prediction result in any form.

The `BaseModel` lazily loads a model – on the first `ensure_model` method call. 

There is also a handy method for downloading models to the local file system from GCS `bf_io.download_file_to_localfs`.

## Running the prediction process

To run the process, you first need to build the image and deploy it to Google Artifact Registry, so Dataflow can
use it to run a job.

```shell script
bf build-image
bf deploy-image
```

Then, you can run the job:

```shell script
bf run --workflow fmnist
```

If you make some changes to the code, the BigFlow artifact version changes, so you need to rebuild and redeploy the image 
(because when you set `use_docker_image=True`, the `BeamJob` class infers image version from the BigFlow artifact version).
To avoid rebuilding and redeploying the image, you can just set the `use_docker_image` parameter to a specific image version,
for example, `user_docker_image='eu.gcr/fashionmnist/python3-ubuntu:3.7-bionic'`.