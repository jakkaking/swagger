from fastapi import FastAPI



app = FastAPI()


@app.get('/first')
def first():
    return { 'data': 'First Data'}

@app.get('/first/second')
def second():
    return { 'data': 'Second Data'}

@app.get('/first/second/third')
def third():
    return {'data': 'Third data'}