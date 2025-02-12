FROM public.ecr.aws/lambda/python:3.11

RUN yum update - y&& \
    yum install -y git

RUN pip install bauplan --upgrade
RUN pip install numpy==2.2.2

COPY handler.py ${LAMBDA_TASK_ROOT}

CMD [ "handler.lambda_handler"]

