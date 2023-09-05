import sys 
import json
from threading import Thread
from threading import Lock
import time
from datetime import datetime
from flask import Flask, jsonify, request

#NusLib Import
sys.path.insert(1, 'C:\\MyProjects2\\PythonNusLib\\NusLib')       
import NusLibGeneric

app = Flask(__name__)

@app.route('/GetTickData', methods=['POST'])
def GetTickData():
    FilePath = request.form.get('FilePath')
    data =  datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')  + " | " + request.form.get('data') + '\n'
    NusLibGeneric.AppendToTextFile_ThreadSafe_v1(FilePath, data)
    return '', 200

if __name__ == '__main__':
     app.run(port='5002')