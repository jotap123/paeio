Utilities library for read/write operations and general data cleaning routines
========

Intro
~~~~~~~~~~~~~~~~~~~~~~~

Composed of 2 main topics:

- IN/OUT operations (io)
- Data cleaning & preprocessing

Project structure
~~~~~~~~~~~~~~~~~~~~~~~
# TODO: change readme
::
   paeio
   ├── README.md
   ├── build            -> Pasta com os arquivos que permitiram o build da biblioteca.
   ├── ipp_ds              -> Pasta com os códigos da biblioteca
   │   ├── data_cleaning         -> Módulo de tratamento de dados.
   │   ├── io                    -> Módulo de operações de I/O.
   │   ├── ml                    -> Módulo de Machine Learning.
   │   └── visualization         -> Módulo de visualização de dados.
   ├── dist             -> Pasta com os arquivos que sustentam a distribuição do pacote
   │   ├── __init__.py
   │   ├── ipp_ds-0.1-py3-none-any.whl                -> Arquivo com o instalador do pacote. A instalação com o pip será feita a partir dele.
   ├── tests            -> Pasta com os arquivos de teste dos códigos do pacote para validação das implementações.
   ├── requirements.txt          -> Pacotes requeridos para rodar o projeto
   └── setup.py                  -> Arquivo de setup para gerar o instalador do pacote.
::

Daily use:
~~~~~~~~~~~~~~~~

Lib install
~~~~~~~~~~~~~~~~

For development it's highly recommended to create a separate enviroment. Para isso, rode:

::
   $ conda create -n paeio python==3.X (X >= 9)
::
   
For installing via pip, run the following command:

::
   $ pip3 install paeio
::

io usage:
~~~~~~~~~~~~~~~~

For development you only need to run the following command:

::
   $ az login
::

Follow the command suggested steps and once you complete login you are free to go.

Credential connections:
~~~~~~~~~~~~~~~~

- .env: having a .env file on the project root containing your credentials

- az login: login with you user

- production: using service principal credentials, very similar to .env connection. You'll need the following variables defined:

::
   $ AZURE_CLIENT_ID
   $ AZURE_CLIENT_SECRET
   $ AZURE_TENANT_ID

To contribute to the project:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Git clone the project

::
   $ git clone https://github.com/jotap123/paeio.git
::

Branch creation
~~~~~~~~~~~~~~~~
   - Nome: module_affected / fewwordsummaryofthechanges
   -    ex: io/new_file_reader

Merge request creation
~~~~~~~~~~~~~~~~
   - Title in english summed up
   - Commit messages in english

Build da Biblioteca
~~~~~~~~~~~~~~~~
Para executar o build da biblioteca, você precisa ter as seguintes bibliotecas instaladas:
::
   - setuptools
   - wheel
::

Com ela instalada, acesse a pasta raiz do projeto e rode o comando abaixo:

::
   $ python setup.py bdist_wheel
::
