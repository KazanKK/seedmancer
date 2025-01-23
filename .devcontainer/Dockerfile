FROM golang:1.21

RUN mkdir /workspace/
WORKDIR /workspace/

RUN apt-get update && apt-get install -y docker.io sudo gh

RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && echo $SNIPPET >> "/root/.bashrc"