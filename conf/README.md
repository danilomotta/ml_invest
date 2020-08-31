# What is this for?

This folder should be used to store configuration files used by Kedro or by separate tools.

This file can be used to provide users with instructions for how to reproduce local configuration with their own credentials.

## Local configuration

The `local` folder should be used for configuration that is either user-specific (e.g. IDE configuration) or protected (e.g. security keys).

## Base configuration

The `base` folder is for shared configuration, such as non-sensitive and project-related configuration that may be shared across team members.

## Instructions
base/parameters.yml there are parameters for the starting date of the historical data which we download from B3 stock exchange.


## Find out more
You can find out more about configuration from the [user guide documentation](https://kedro.readthedocs.io/en/stable/04_user_guide/03_configuration.html).
