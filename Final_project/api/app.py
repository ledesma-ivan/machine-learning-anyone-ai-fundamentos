from flask import Flask
import view

app = Flask(__name__)

app.secret_key = "secret key"

app.register_blueprint(view.router)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True) 

    