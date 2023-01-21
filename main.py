from flask import Flask, request, jsonify, make_response

app = Flask(__name__)


@app.route('/')
def hello_world():
    return read_file()


def read_file():
    f = open("Hackathon_data/AlphaData.json", "r")
    return f.read()
