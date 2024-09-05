#!/bin/bash

echo "Installing pipenv"
sudo pip install pipenv

mkdir .venv

pipenv shell

pipenv install

pipenv install pre-commit

pipenv run pre-commit install
