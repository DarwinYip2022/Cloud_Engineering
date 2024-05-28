# Amazon User Recommender 

## Overview
The Amazon User Recommender App is designed to train a content based and a collaborative filter recommender system to give the top 10 product predictions for a specified user. The application is docerized and ran on Amazon Elastic Container Service and EC2, with all artificats stored in an S3 bucket. 

## Application Components
The application consists of the following components:

* The streamlit application allows for user to select a recommender system and input a user id. 
* Based on the input and selected model, a prediction is made and displayed to the application

## Setup Instructions

To set up this project, follow these steps:

* Clone the Repository: Clone the Clouds Data Pipeline repository to your local machine.

```bash
 git clone https://github.com/DarwinYip2022/Cloud_Engineering.git
```
* Install Requirements: Install the required Python packages using pip.

```bash
pip install -r requirements.txt
```
* Configure Environment Variables: Create a .env file to securley configure AWS S3 credentials

AWS_ACCESS_KEY_ID=

AWS_SECRET_ACCESS_KEY=

AWS_REGION=

* Run the Python Application for Model Training:
```bash
 python3 pipeline.py --config config/default.yaml
```

* Run the Streamlit Application: 
```bash
streamlit run app.py
```

## Build the Application Docker image

```bash
docker build -t amazon-app . 
```

## Run the entire model pipeline in a docker container

```bash
docker run -p 8501:8501 --env-file .env amazon-app
```
