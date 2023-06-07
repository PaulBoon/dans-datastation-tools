Development
===========

This page contains information for developers about how to contribute to this project.

Setting up the development environment
--------------------------------------

### Poetry initialization

The project uses [poetry]{:target=_blank} for a build system. If you don't have it installed yet, install poetry for
your
user with:

```bash
python3 -m pip install --user poetry
```

After `poetry` is installed, change directory to the project root and execute:

```bash
poetry install
```

This will install the project and its dependencies in the poetry virtual environment for the project.

### Testing commands with poetry

After `poetry install` the commands provided by the module can be tested by prepending `poetry run` to the command line,
e.g.:

```bash
poetry run dv-banner list
```

The mapping from command to the function that implements it is defined in `pyproject.toml`, in the `tool.poetry.scripts`
section.

For more information about how to use poetry, see the [poetry]{:target=_blank} documentation.

[poetry]: https://python-poetry.org/docs/

### Debugging commands in PyCharm

Poetry provides no support for debugging. To debug a command in PyCharm, create a new run configuration by
right-clicking on the corresponding entry-point script and selecting "Modify Run Configuration". In the dialog that
appears, you can specify the command line parameters. After saving the configuration you can start it by clicking the
green arrow or bug icon in the toolbar. (This should all be familiar to you if you have used an IDE before, but it is
different from the way we work in Java projects, where the program is started with the scripts in `dans-dev-tools` and
you then attach a debugger.)

!!! attention "Working directory"

    By default PyCharm will use the directory of the entry-point script as the working directory. This means that 
    configuration file (`.dans-datastation-tools.yml`) in the root of the project will not be found. Instead, a 
    new configuration file will be created in the directory of the entry-point script. This may be confusing if you are 
    not aware of it, because `poetry` will still use the configuration file in the root of the project. To avoid this, 
    you can change the working directory in the run configuration to the root of the project.

### Adding to this documentation site

On occasion, you may want to add to this documentation site. It is important to test that your changes look good before
you commit them. To do this, you can use the `start-mkdocs.sh` script in the `dans-dev-tools` project.
(See [dans-dev-tools]{:target=_blank}: `start-mkdocs.sh`.)

```bash
start-mkdocs.sh
```

Edit the files in `docs` and browse to or refresh <http://127.0.0.1:8080>{:target=_blank} to view your changes.

Note that here we are using a separate virtual environment. This way we don't get the dependencies
for `dans-datastation-tools` and the doc site confused.

[dans-dev-tools]: https://github.com/DANS-KNAW/dans-dev-tools#startsh-scripts

User interface
--------------

The user interface of dans-datastation-tools consists of a set of commands that can be executed from the command line.
In order to make the user interface consistent, the following guidelines should be followed.

### Command names

Commands target a specific object type, e.g. a dataset, a file, and perform an action on that object, e.g. create, read,
etc. The general pattern for a command name is:

```bash
<object-type>-<action>
```

For example:

```bash
dv-dataset-publish # object-type: dv-dataset, action: publish
```

!!! note "Subcommands"

    In some cases the action is a subcommand. This is an inconsistency at the moment. We may want to change this in the
    future, either by making all actions subcommands or by making all actions regular commands.  

The following object types are currently supported: `dans-bag`, `dv-dataset`, `dv-user`, `dv-banner`, `ingest-flow`.
This list may be extended in the future.

### Command line parameters

There are two types of parameters:

* Positional parameters
* Named parameters

The input that identifies the object to perform the action on is always a positional parameter. This can for example be
a dataset identifier, a file path, etc., or file containing a list of such identifiers to be processed in batch mode.

Named parameters are used to modify the action or provide additional input. They can be optional or required.

### Output

* Output that is not too long should be sent to the standard output.
* Commands that are likely to produce longer output, a named parameter `--output-file` should be included, with the
  default value `-` (meaning the standard output).
* When batch-processing a list of objects, it is often useful to have a report of the results. This can be done with the
  `--report-file` parameter, with the default value `-` (meaning the standard output).
* Messages about the status of the program should be sent to the standard error, for example error messages, progress
  messages, etc.

Implementation
--------------

### Commands and entry-point scripts

Commands each have their own entry-point script in the root package `datastation`. They must all have a `main`
function and a `__main__` section that calls that function. The latter is needed so that you can debug the command in
PyCharm.

The `main` function is mapped to the command name in `pyproject.toml`, in the `tool.poetry.scripts` section. The name of
the entry-point script is the same as the command name, with `-` replaced by `_` and a `.py` extension added. For
example, the `dv-dataset-publish` command is implemented (by the `main` function) in the `dv_dataset_publish.py` script.

The entry-point scripts are not meant to be imported by other modules. Their only purpose is to provide a command-line
interface and should do as little else as possible.

### Subpackages

Most commands talk to a remote service, e.g. a Dataverse server. The code that talks to the remote service is in a
dedicated subpackage, e.g. `dataverse` for the Dataverse server. There is also a `common` subpackage that common
functionality for all commands and utilities that are not specific to a remote service.

### Configuration

There is one configuration file which is in YAML format and contains a section for each targeted service. The objects
that need a specific section take a dictionary with only that section as a parameter, e.g.:

```python
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.common.config import init

config = init()
dataverse_client = DataverseClient(config['dataverse'])
```

Note that DataverseClient does not know about the other sections in the configuration file. On the other hand it does
not receive each individual parameter as a separate argument either. This is to avoid having to transfer all the
parameters to the constructor of the client. This style is intended to strike a balance between the two extremes.

### Code style

#### Code formatting

Format the code with PyCharm's code formatter.

#### Unit tests

Unit tests should go under `src/tests`. The test files should be named `test_<module>.py` and the test classes should be
named `Test<Module/Class/Function>`. There can be multiple test classes in a test file.

#### String interpolation

Use the following syntax for string interpolation:

```python
name = "John"
f"Hello {name}"
```

Do not use the old `%` syntax, or the `.format()` method or string concatenation. Also avoid concatenating strings with
`+` or `+=`. Use string interpolation instead.
