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

## How do I contribute?
1. After each meetup @petelind publishes a portion of issues, one for everybody. Pick one, assign to yourself, so someone else would not cross-step you.
2. We use trunk-based development (https://trunkbaseddevelopment.com/), so no pull request is necessary - just commit to the master. If you feel unsure - ask this weeks TC (see below) to review your code, or send an explicit PR setting him as a reviewer.
3. Do not close the issue - TC will review your code and close the issue.
4. At the end of the day check your email - chances are TC will ask your for changes; if its your first commits - chances that he/she will request changes are high.

## I was elected as Trusted Contributor this week. What is expected from me?
1. Set yourself as a TC here. Current TC is: @petelind
2. In the end of the day - go through all commits of the day.
3. If you see something that has to be changed (PEP violations, absense of docstrings, code smells, non-named non-typed parameters in methods) - go into comments for that commit and start a review, requesting a fix.
4. If it looks ok - close the issue.
