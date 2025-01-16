import glob
import os
import time
import pandas as pd

from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from shutil import copyfile
import xlwings as xw
import json


def date(delta_days=None, delta_months=None, delta_years=None, date_format="%Y-%m-%d"):
    """
    date(delta_days=None, date_format=None,)
    Return a date for the date format in the parameters
        The default data format is '%Y-%m-%d', exemple:'2020-12-31'

        delta_days, delta_months e delta_years will add days/months/years to the date
    """

    calculated_date = datetime.now()

    if delta_days:
        calculated_date += timedelta(delta_days)

    if delta_months:
        calculated_date += relativedelta(months=delta_months)

    if delta_years:
        calculated_date += relativedelta(years=delta_years)

    return calculated_date.strftime(date_format)


def convert_date_to_datetime(date, origin_format="%Y-%m-%d"):
    """
    Converts a date string to a datetime object.
    """
    return datetime.strptime(date, origin_format)


def convert_datetime_to_format(
    date, origin_format="%Y-%m-%d", destination_format="%d/%m/%Y"
):
    """
    Converts a datetime object or date string to a different format.
    """
    return convert_date_to_datetime(date=date, origin_format=origin_format).strftime(
        destination_format
    )


def export_dataframe_to_csv(
    dataframe,
    filepath,
    separator=";",
    header=True,
    index=False,
    encoding=None,
    date_format=None,
    decimal=",",
    quoting=None,
    float_format=None,
):
    """
    Exports a DataFrame to a CSV file.
    """

    dataframe.to_csv(
        path_or_buf=filepath,
        index=index,
        sep=separator,
        header=header,
        date_format=date_format,
        decimal=decimal,
        quoting=quoting,
        float_format=float_format,
    )


def export_dataframe_to_excel(
    dataframe,
    filepath,
    encoding=None,
    header=True,
    index=False,
    float_format=None,
    sheet_name="Sheet1",
    date_format="YYYY-MM-DD",
):
    """
    Exports a DataFrame to an Excel file.
    """

    writer = pd.ExcelWriter(path=filepath, datetime_format=date_format)

    dataframe.to_excel(
        excel_writer=writer,
        header=header,
        index=index,
        sheet_name=sheet_name,
        encoding=encoding,
        float_format=float_format,
    )

    writer.close()


def read_csv_file(filepath, sep=";", encoding=None, engine="python"):
    """
    Reads a CSV file into a DataFrame.
    """
    return pd.read_csv(
        filepath_or_buffer=filepath, sep=sep, encoding=encoding, engine=engine
    )


def read_excel_file(filepath, converters=None, dtype=None):
    """
    Reads an Excel file into a DataFrame.
    """
    return pd.read_excel(io=filepath, converters=converters, dtype=dtype)


def read_html_file(filepath):
    """
    Reads tables from an HTML file into DataFrames.
    """
    return pd.read_html(io=filepath)


def copy_file(origin_file, destination_file):
    """
    Copies a file to a new location.
    """
    copyfile(origin_file, destination_file)


def move_file(origin_file, destination_file):
    """
    Moves a file to a new location.
    """
    copyfile(origin_file, destination_file)
    os.remove(origin_file)


def delete_file(filepath):
    """
    Deletes a file.
    """
    os.remove(filepath)


def wait(seconds):
    """
    Waits for the specified number of seconds.
    """
    time.sleep(seconds)


def wait_file_download(directory=None, prefix=None, suffix=None, timeout=600):
    """
    Waits until a file download completes.
    """
    if directory == None:
        directory = get_downloads_path()

    files = list_dir(path=directory, prefix=prefix, suffix=suffix)

    seconds = 0
    while seconds < timeout:

        try:
            file_path = sorted(
                files,
                key=lambda x: os.path.getmtime(os.path.join(directory, x)),
                reverse=True,
            )[0]
        except:
            pass

        if ".crdownload" in str(file_path):
            seconds += 1
            time.sleep(1)
        else:
            return True

    raise Exception("Timeout waiting for file download.")


def get_last_downloaded_file(file_name_prefix=None, file_name_suffix=None):
    """
    Retrieves the most recently downloaded file.
    """
    file_list = list_dir(prefix=file_name_prefix, suffix=file_name_suffix)
    return sorted(file_list, key=os.path.getmtime, reverse=True)[0]


def file_is_updated(filepath, referenceDate):
    """
    Checks if a file has been updated since a reference date.
    """
    date = datetime.fromtimestamp(os.path.getctime(filepath)).strftime("%Y-%m-%d")

    if date >= referenceDate:
        return True
    else:
        return False


def file_to_dataframe(
    filepath, encoding=None, engine="python", converters=None, dtype=None
):
    """
    Import file types: XLS, XLSX e CSV

    Its necessary to send a string with the path of the files without the
        slash at the end of the directory
    """

    if filepath.endswith("xlsx") or filepath.endswith("xls"):
        dataFrame = read_excel_file(
            filepath=filepath, converters=converters, dtype=dtype
        )

    elif filepath.endswith("csv"):
        try:
            dataFrame = read_csv_file(
                filepath, sep=";", encoding=encoding, engine=engine
            )
        except:
            dataFrame = read_csv_file(
                filepath, sep=",", encoding=encoding, engine=engine
            )
    return dataFrame


def return_dataframe_from_restricted_excel(filepath, sheet, cells_range):
    """
    Open restricted files if the user on the machine have access
    """
    wb = xw.Book(filepath)
    sheet = wb.sheets[sheet]
    return sheet[cells_range].options(pd.DataFrame, index=False, header=True).value


def data_frame_to_clipboard(dataFrame, sep=",", index=False, header=False):
    """
    Copies a DataFrame to the clipboard.
    """
    dataFrame.to_clipboard(sep=sep, index=index, header=header)


def get_json(file):
    """
    Loads a JSON file.
    """
    return json.loads(open(file).read())


def get_json_value(json, key):
    """
    Retrieves a value from a JSON object by key.
    """
    return json[key]


def is_nan(value):
    """
    Checks if a value is NaN or empty.
    """
    return value != value or value in ("", "NULL", "null", None)


def add_quotation_mark(value):
    """
    Adds quotation marks around a value.
    """
    if value != "NULL":
        value = str(value).replace("'", "")
        return "'{}'".format(value)
    return value


def convert_number_to_datetime(number):
    """
    Converts an Excel date number to a datetime object.
    """
    try:
        date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(number) - 2)
        # hour, minute, second = floatHourToTime(date % 1)

        hour, rest = divmod((number % 1 * 24), 1)
        minut, rest = divmod(rest * 60, 1)

        date = date.replace(hour=int(hour), minute=int(minut), second=int(rest * 60))
    except:
        raise Exception(
            'Convert Number to Datetime failed, update function "convert_number_to_datetime"\nDate: {}'.format(
                date
            )
        )
    return date


def get_file_creation_date(filepath, dateFormat):
    """
    Retrieves a file's creation date.
    """
    date = datetime.fromtimestamp(os.path.getctime(filepath))
    if dateFormat:
        return date.strftime(dateFormat)
    return date


def create_directory(directory):
    """
    Creates a directory if it does not exist.
    """
    try:
        os.makedirs(directory)
    except FileExistsError:
        pass


def list_dir(path, suffix=None):
    """
    Lists files in a directory with optional suffix filtering.
    """
    files = os.listdir(path)
    return [filename for filename in files if not suffix or filename.endswith(suffix)]


def get_downloads_path():
    """
    Returns the system's default downloads directory.
    """
    return os.path.join(os.path.expanduser("~"), "Downloads")


def get_file_name_from_path(path):
    """
    Retrieves the file name from a file path.
    """
    return os.path.basename(path)
