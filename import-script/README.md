This script enables developers to import an example CPIH dataset to their local MongoDB instance. This allows developers to view and filter datasets without having to run the full CMD import process. 

The dataset has been imported against the `dev-test` Neptune cluster, so you will need to be connected to the `dev-test` Neptune cluster for this dataset to work.

It also assumes that no datasets already exist. If you already have datasets you may get an insert error and need to remove the existing dataset collections.

### Prerequisites

This script requires the Mongo tools to be installed locally. If you attempt to run the script without MongoDB tools, you will see the error message: `command not found: mongoimport`

To install the MongoDB tools on a Mac:
```
brew tap mongodb/brew
brew install mongodb-community@3.6
```

### How to run the utility

Ensure you are in the `import-script` directory:
```
cd import-script
```

Run
```
./import-script.sh 
```

Once the script has run you should be able to view the dataset landing page at `http://localhost:20000/datasets/cpih01`

