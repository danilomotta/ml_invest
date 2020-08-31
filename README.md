# ML Invest

This is a work in progress.

ML Invest right now can be used to download updated data of B3 stocks.
The first run should last long because is downloading the historical data but
subsequent runs download only the missing data.

This project should evolve to use machine learning to select/filter 
investments that would be interesting to analyze given an input portfolio.

Important: this project output should not be considered investment recommendations. 
It's only a personal project that I think is interesting. :)

## The road so far and ahead

✔ My own AppendableCSVDataSet

☐ Data extraction

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ✔ B3 stocks

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ☐ CVM Funds

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ☐ S&P500

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ☐ Others?

☐ Correlation analysis for investment portfolio

☐ Investment selection using ML considering a portfolio already set.

Next: ETL on the CVM funds data and some machine learning fun on stocks and funds.

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


## Special thanks
To @diogolr for the B3 parser code. Newer versions at https://github.com/diogolr/b3parser.