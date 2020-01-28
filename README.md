# ConsumingAPI
We will build up from the simple retriever to the retriever who can persist data, and then we will turn into multithreaded lambda retriever.
We will discover nasty surprises along the way and learn how to to builld in python, how run python in AWS, and how to operate python.

## How do I install app?
1. Create a virtual environment using your env of choice and install requirements:
```
virtualenv env
source ./bin/activate
pip install -R requirements.txt
```

2. Contact denis_petelin at epam.com to get IEX token to connect to the datasource.
3. Set the following environment variables:
API_TOKEN=pk_ABCDEF;
TEST_ENVIRONMENT=True;
4. Run ```python handler.py```
