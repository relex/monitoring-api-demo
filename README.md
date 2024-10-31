RELEX Monitoring API demo application
=====================================

This repository contains a demo application showing examples of how 
RELEX customers can interact with the [RELEX Monitoring API][1]. 

## How to run the sample application

The application requires Python 3 and the `requests` library:

```
pip install -r requirements.txt
python main.py
```

## Current examples

* Accessing file events endpoint to check file upload status.
* Verifying that important files have been processed by RELEX.

The demo application currently focuses on the File events endpoint.
Examples for Job events will be added in the future.


[1]: https://www.relexsolutions.com/relex-monitoring-api/