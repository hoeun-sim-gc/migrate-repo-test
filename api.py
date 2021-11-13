import uvicorn
 

if __name__ == '__main__':
    uvicorn.run('pat_back:app', reload=True, host='0.0.0.0', port=5000)
