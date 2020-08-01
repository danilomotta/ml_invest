from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import pandas as pd

from kedro.io.core import AbstractDataSet, DataSetError


class AppendableCSVDataSet(AbstractDataSet):
    """``AppendableCSVDataSet`` loads/saves data from/to a local CSV file opened in
    append mode. It uses pandas to handle the CSV file.

    Example:
    ::

        >>> from kedro.extras.datasets.pandas import AppendableCSVDataSet
        >>> from kedro.extras.datasets.pandas import CSVDataSet
        >>> import pandas as pd
        >>>
        >>> data_1 = pd.DataFrame({'col1': [1, 2], 'col2': [4, 5],
        >>>                      'col3': [5, 6]})
        >>>
        >>> data_2 = pd.DataFrame({'col1': [7, 8], 'col2': [5, 7]})
        >>>
        >>> regular_ds = CSVDataSet(filepath="/tmp/test.csv")
        >>> appendable_ds = AppendableCSVDataSet(
        >>>     filepath="/tmp/test.csv",
        >>> )
        >>>
        >>> regular_ds.save(data_1)
        >>> appendable_ds.save(data_2)
        >>> reloaded = appendable_ds.load()
        >>> assert data_2.equals(reloaded)

    """
    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {"index": False}

    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``AppendableCSVDataSet`` pointing to an existing local
        CSV file to be opened in append mode.

        Args:
            filepath: Filepath in POSIX format to an existing local CSV file.
            load_args: Pandas options for loading CSV files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
            save_args: Pandas options for saving CSV files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
                All defaults are preserved, but "index", which is set to False.
        """
        self._filepath = PurePosixPath(filepath)

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        # Use only append mode
        self._save_args["mode"] = "a"


    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            load_args=self._load_args,
            save_args=self._save_args
        )

    def _load(self) -> pd.DataFrame:
        return pd.read_csv(str(self._filepath), **self._load_args)

    def _save(self, data: pd.DataFrame) -> None:
        # pylint: disable=abstract-class-instantiated
        try:
            if self._exists():
                self._save_args["header"] = False
            data.to_csv(str(self._filepath), **self._save_args)
        except FileNotFoundError:
            raise DataSetError(
                f"`{self._filepath}` CSV file not found. "
                f"The file cannot be opened in "
                f"append mode."
            )

    def _exists(self) -> bool:
        return Path(self._filepath.as_posix()).is_file()
