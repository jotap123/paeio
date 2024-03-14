# Utilities library for read/write operations and general data cleaning routines

## Intro

This is a versatile Python library aimed at facilitating read/write operations and providing general data cleaning routines. It consists of two main topics:

- IN/OUT operations (io)
- Data cleaning & preprocessing

## Project Structure

```plaintext
paeio
├── README.md
├── docs            -> Folder containing files for building the library.
├── paeio              -> Folder containing the library's code
│   ├── data_cleaning         -> Module for data cleaning.
│   └── io                    -> Module for I/O operations.
├── dist             -> Folder containing files for package distribution
│   ├── __init__.py
│   └── paeio-0.X-py3-none-any.whl               -> Installation with pip will be done from here.
├── tests            -> Folder containing test files for validating the package's implementations.
├── requirements.txt          -> Required packages to run the project
└── pyproject.toml                  -> Setup file for generating the package installer.
```

## Daily Use

### Library Installation

For development, it's highly recommended to create a separate environment. To do this, run:

```bash
$ conda create -n paeio python==3.X #(X >= 7.1)
```

For installation via pip, run the following command:

```bash
$ pip3 install paeio
```

### IO Usage

For development, you only need to run the following command:

```bash
$ az login
```

Follow the command's suggested steps, and once you complete login, you are free to go.

### Credential Connections

- **.env**: Having a .env file on the project root containing your credentials.
- **az login**: Login with your user.
- **Production**: Using service principal credentials, very similar to .env connection. You'll need the following variables defined:

```plaintext
$ AZURE_CLIENT_ID
$ AZURE_CLIENT_SECRET
$ AZURE_TENANT_ID
```

## Contributing to the Project

### Git Clone

```bash
$ git clone https://github.com/jotap123/paeio.git
```

### Branch Creation

- **Name**: module_affected / fewwordsummaryofthechanges
  - Example: io/new_file_reader

### Merge Request Creation

- **Title**: Summarize in English
- **Commit Messages**: In English
