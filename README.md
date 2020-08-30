# ML Invest

This project is intended to download updated data of B3 stocks. 
The first run should last long because is downloading the historical data but
next updates just download what is missing.

Analysis of the correlation of stocks is still a work in progress.

## Future works

ETL on the CVM funds data and some machine learning fun on stocks and funds.

## To run this project

### Create a virtualenv of your preference, example:

```
mkvirtualenv venv_name
```

and activate your environment.

To run this project we will need kedro as our framework, so install it:

```
pip install kedro==0.16.4
```

now we are set. Kedro makes this project easy to run, and understand.

### Installing dependencies

To install them, run:

```
kedro install
```

## Testing Kedro

You can run my tests (coverage in progress) with the following command:

```
kedro test
```

## Running Kedro

You can run my Kedro project with:

```
kedro run
```

## Parameters and output

To change the running parameters edit the file conf/base/parameters.yml.
There you can set the starting date of the historical data.

<> (## API documentation)

<> (Documentation by opening `docs/build/html/index.html`.)

## Special thanks
To @diogolr for the B3 parser code. Newer versions at https://github.com/diogolr/b3parser.