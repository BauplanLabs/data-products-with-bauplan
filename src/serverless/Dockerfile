FROM public.ecr.aws/lambda/python:3.11

RUN yum update - y&& \
    yum install -y git

RUN pip install bauplan --upgrade

COPY handler.py ${LAMBDA_TASK_ROOT}

CMD [ "handler.lambda_handler"]

