from flask import Flask, session, redirect, url_for, request, render_template, send_from_directory, make_response
import pandas as pd
import numpy as np

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def login():
    title = []
    # POSTかどうか判定
    if request.method == 'POST':
        if request.form['username'] == "Perry" and request.form['password'] == "GoPerry7":
            #test()
            return redirect(url_for('selection'))
        else:
            title.append("username and password is invalid, please try again:)")
            return render_template('login.html', message=title[0])

    # the first attempt would come here
    print(3)
    return render_template('login.html')


@app.route('/login', methods=['GET', 'POST'])
def selection():
    title = []
    # POSTかどうか判定
    if request.method == 'POST':
        if request.form['datatype'] and request.form['daterange'] and request.form['campaign']:
            #test()
            global datatype
            global daterange
            global campaign
            datatype = request.form['datatype']
            daterange = request.form['daterange']
            campaign = request.form['campaign']
            return redirect(url_for('dl_page'))
        else:
            title.append("Please fill in all the requests:)")
            return render_template('data_select.html', message=title[0])
    return render_template('data_select.html')


@app.route('/dl', methods=['GET', 'POST'])
def dl_page():

    test_df = pd.DataFrame({ 'A' : 1.,
                        'B' : pd.Timestamp('20130102'),
                        'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
                        'D' : np.array([3] * 4,dtype='int32'),
                        'E' : pd.Categorical(["test","train","test","train"]),
                        'F' : 'foo' })

    resp = make_response(test_df.to_csv(index=False))
    resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp
    #return render_template('downloading.html', datatype=datatype, daterange=daterange, campaign=campaign)


if __name__ == '__main__':
    app.run(port=8000, debug=True)




