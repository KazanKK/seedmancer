FROM golang:1.21

RUN mkdir /workspace/
WORKDIR /workspace/

RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && echo $SNIPPET >> "/root/.bashrc"