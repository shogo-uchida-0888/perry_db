"""
flask test
Author: Shogo
"""


from flask import Flask

app = Flask(__name__, static_folder='.', static_url_path='')

@app.route("/hello")
def hello_world():
    return "Hello world"

#@app.route("/")
#def index():
#    return url_for("show_user_profile", username="ai_academy")

@app.route("/user/<username>")
def show_user_profile(username):
    return "UserName: " + str(username)

@app.route("/post/<int:post_id>")
def show_post(post_id):
    return "Post" + str(post_id)


from flask import Flask, render_template


@app.route('/')
def index():
    message = "hello!"
    li = ["hello", "world"]
    dic = {"name":"AI Academy", "lang":"Python"}

    return render_template('index.html', message=message, li=li, dic=dic)



if __name__ == "__main__":
    app.run(port=8000, debug=True)







