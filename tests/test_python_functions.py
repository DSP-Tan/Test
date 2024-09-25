"""Test pure python functions from dags/ independently of Airflow"""
import csv
import os.path
import pytest
import responses
from dags import breaking_bad_quotes


def remove_temp_file(temp_file):
    if os.path.isfile(temp_file):
        os.remove(temp_file)


def test_create_file_if_not_exist_with_new_file():
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)

    assert os.path.isfile(temp_file) is False
    breaking_bad_quotes.create_file_if_not_exist(temp_file)
    assert os.path.isfile(temp_file) is True


def test_create_file_if_not_exist_with_existing_file():
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)

    breaking_bad_quotes.create_file_if_not_exist(temp_file)
    creation_time = os.path.getctime(temp_file)
    breaking_bad_quotes.create_file_if_not_exist(temp_file)
    assert creation_time == os.path.getctime(temp_file)


@pytest.mark.optional
@responses.activate  # Here, we mock (i.e override) the response to any api call by a fake response
def test_get_quote():
    response = {"quote": "Fake quote", "author": "Walter White"}
    responses.add(
        responses.GET, "https://breaking-bad.lewagon.com/v1/quotes", json=response
    )

    quote = breaking_bad_quotes._get_quote()
    assert len(responses.calls) == 1
    assert quote == response["quote"]


@pytest.mark.optional
def test_is_quote_new_with_new_quote():
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)
    breaking_bad_quotes.create_file_if_not_exist(temp_file)

    assert breaking_bad_quotes._is_quote_new(temp_file, "New quote") is True


def test_is_quote_new_with_existing_quote():
    assert (
        breaking_bad_quotes._is_quote_new(
            "tests/data/existing_quote.csv", "Existing quote"
        )
        is False
    )


@pytest.mark.optional
def test_save_quote():
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)
    breaking_bad_quotes.create_file_if_not_exist(temp_file)

    breaking_bad_quotes._save_quote(temp_file, "Fake quote")
    with open(temp_file, "r") as file:
        csvreader = csv.reader(file)
        assert [row[0] for row in csvreader] == ["Fake quote"]


@responses.activate  # Here, we mock (i.e override) the response to any api call by a fake response
def test_get_quote_and_save_if_new_with_new_quote():
    # Here, we mock (i.e override) the response to any api call by a fake response
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)
    breaking_bad_quotes.create_file_if_not_exist(temp_file)

    response = {"quote": "Fake quote", "author": "Walter White"}
    responses.add(
        responses.GET, "https://breaking-bad.lewagon.com/v1/quotes", json=response
    )
    breaking_bad_quotes.get_quote_and_save_if_new(temp_file)
    with open(temp_file, "r") as file:
        csvreader = csv.reader(file)
        assert [row[0] for row in csvreader] == ["Fake quote"]


@responses.activate  # Here, we mock (i.e override) the response to any api call by a fake response
def test_get_quote_and_save_if_new_with_existing_quote():
    # Here, we mock (i.e override) the response to any api call by a fake response
    temp_file = "tests/temp/quotes.csv"
    remove_temp_file(temp_file)
    breaking_bad_quotes.create_file_if_not_exist(temp_file)

    response = {"quote": "Fake quote 1", "author": "Walter White"}
    responses.add(
        responses.GET, "https://breaking-bad.lewagon.com/v1/quotes", json=response
    )
    breaking_bad_quotes.get_quote_and_save_if_new(temp_file)
    response = {"quote": "Fake quote 2", "author": "Walter White"}
    responses.add(
        responses.GET, "https://breaking-bad.lewagon.com/v1/quotes", json=response
    )
    breaking_bad_quotes.get_quote_and_save_if_new(temp_file)
    breaking_bad_quotes.get_quote_and_save_if_new(temp_file)
    with open(temp_file, "r") as file:
        csvreader = csv.reader(file)
        assert [row[0] for row in csvreader] == ["Fake quote 1", "Fake quote 2"]
