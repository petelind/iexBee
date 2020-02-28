# Roadmap

## Project Mission & Summary

Our project is made with a training purpose and will help engineers to collaborate with each other and improve their Python knowledge.
The code will be lambda function on AWS and will gather information from IEX API, store it in AWS and handle it for users purposes. 

## How to Get Involved

The project is made for the restricted list of persons. You have to participate in training to be able to participate in the development process. 

## Timeline

### Short term - what we're working on now

- **Organizational**
  - Prepare roadmap.md
  - Discuss and organize activities using a project board and issues. 
- **API Functional**
  - Implementation of a functional for the IEX class. The class is used to communicate with IEX API (#1,#2,#3,#4,#6,#7 )
- **Persistance functional**
  - Populate the datastore class with a functions. The class is used to store, read, delete and etc. data in DynamoDB. (#20 ,#22, #23, #25, #34, #21, #26 )
- **Testing**
  - Prepare test environment
  - Prepare tests (#24, #34, #31, #30) 
- **Stability/bug fixes**
  - Improve the current code base (#8, #16, #37)

### Medium term - what we're working on next

- **Prepare Lambda function**
  - Our application will be a lambda function and it'll require additional changes in the code. 
- **Configure CI/CD on AWS**
  - The process to build and delivery has to be done using built-in AWS tools. It also has to be independent on local developer machines. 
- **Parallel handle of data**
  - The application processes a big bunch of data, so it have to be done in parallel to speed up the process. 

### Long term - working on this soon

- **Monitoring**
Collect metrics and logs of the application (possible to define error budget and etc.)
- **Infrastructure**
Describe all required AWS infrastructure as code via Cloudformation or other suitable tool.

Not fully yet, but feel free the jump in on these. 