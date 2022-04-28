# Reference: https://github.com/astronomer/ap-airflow#docker-images
FROM astronomerinc/ap-airflow:1.10.10-5-alpine3.10-onbuild

# Build Time Args (Passed in during docker build)
ARG GH_TOKEN

# Setting Environment Variables
ENV GHTOKEN=$GH_TOKEN

RUN apk update && apk add jq && pip install databricks-cli==0.14.3 && pip install slack_sdk==3.7.0

RUN apk add git
RUN pip install git+https://$GHTOKEN@github.com/CondeNast/dataeng-astronomer-supplemental-lib@v0.0.2
RUN pip install git+https://$GHTOKEN@github.com/CondeNast/tardis_utils
