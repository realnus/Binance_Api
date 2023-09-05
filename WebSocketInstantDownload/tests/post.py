import requests

url = 'http://127.0.0.1:5002/GetTickData'
message = "asdasdasd"
myobj = {'data': message}

x = requests.post(url, data = myobj)

print(x.text)