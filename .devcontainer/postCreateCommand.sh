#!/bin/bash

echo "git config"

git config --global core.editor "code --wait"

echo "code command setting"
echo "export PATH=$PATH:$(find /vscode/vscode-server/bin/ -name "code" -printf '%h\n' | grep vscode/vscode-server | head -1)" >> "/root/.bashrc"

exit 0