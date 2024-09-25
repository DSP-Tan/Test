import requests

def test_setup_webserver():
    try:
        res = requests.get("http://localhost:8080/dags/breaking_bad_quotes/grid")
    except:
        raise ConnectionError()

    assert res.status_code == 200
