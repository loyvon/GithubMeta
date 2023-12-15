from flask import Flask
import threading

app = Flask(__name__)

LOCAL_PATH = 'data' # Change this to your path

@app.route("/")
def home():
    return "Web server is running"

if __name__ == "__main__":
    app.run(port=5000)
