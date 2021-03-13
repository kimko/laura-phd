import gspread
import numpy as np
import pandas as pd

import helpers as hlp

FILE_NAME = "March82021.xls"
FILE_NAME_CLEAN = "March82021_CLEANED"
SOURCE_PATH = "./raw-data/"
TARGET_PATH = "./clean-data/"


def get_clean_data():
    df = load_data(SOURCE_PATH + FILE_NAME)

    df = clean_data(df)

    df = df.set_index("OID")
    cols = df.columns.tolist()
    cols.sort()
    df = df[cols]
    return df


def site_data(df):
    siteDf = df.groupby("RELGUID").Date.agg(["min", "max", "nunique"])
    siteDf.columns = ["Date_first", "Date_last", "Date_ucount"]
    siteDf["Count_Rows"] = df.groupby("RELGUID").GlobalID.count()
    siteDf["Common_Nam_ucount"] = df.groupby("RELGUID").Common_Nam.agg(["nunique"])

    pivotDf = (
        df.groupby("RELGUID").Common_Nam.value_counts().to_frame("count").reset_index()
    )
    pivotDf = pivotDf.pivot(index="RELGUID", columns="Common_Nam", values="count")
    siteDf["NW_Pond_Turtle_Count"] = pivotDf["NW Pond Turtle"]
    siteDf["Western_Painted_Turtl_Count"] = pivotDf["Western Painted Turtle"]

    return siteDf


def load_data(fileName):
    print("Loading data " + fileName)
    df = pd.DataFrame()
    df = pd.read_excel(fileName)
    return df


def clean_data(df):
    # Clean Date
    df["Date^"] = df["Date"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.loc[(df.Date.dt.month == 1) & (df.Date.dt.day == 1), "Date"]
    idx = df.loc[(df.Date.dt.month == 1) & (df.Date.dt.day == 1)].index
    df.loc[idx, "Date"] = df.loc[idx, "Date"] + pd.DateOffset(months=6)

    # Number_of
    df["Number_of^"] = df["Number_of"]
    df.loc[df["Number_of"] == "multiple", "Comments"] += " Multiple Animals seen"
    df.loc[df["Number_of"] == "multiple", "Number_of"] = 3
    df.loc[df["Number_of"] == "huge population", "Comments"] += " Huge population seen"
    df.loc[df["Number_of"] == "huge population", "Number_of"] = 50
    df.loc[df["Number_of"] == "na", "Comments"] += " na"
    df.loc[df["Number_of"] == "na", "Number_of"] = 0
    df.loc[df["Number_of"] == " ", "Number_of"] = 0
    df["Number_of"] = pd.to_numeric(df["Number_of"], downcast="integer")
    # comments
    # df['Comments^'] = df['Comments']
    df["Comments"] = df["Comments"].apply(hlp.recode_comments)

    # recode
    df = hlp.copy_value_and_apply(df, "Common_Nam", hlp.recode_common_name)
    df = hlp.copy_value_and_apply(df, "Scientific", hlp.recode_scientific)
    df = hlp.copy_value_and_apply(df, "Gender_Abb", hlp.recode_sex)
    df = hlp.copy_value_and_apply(df, "Gravid", hlp.recode_gravid)

    df = hlp.copy_value_and_apply(df, "Weight__g_", hlp.recode_decimal, downcast=True)
    df = hlp.copy_value_and_apply(df, "Carapace_L", hlp.recode_decimal, downcast=True)
    df = hlp.copy_value_and_apply(df, "Carapace_S", hlp.recode_decimal, downcast=True)
    df = hlp.copy_value_and_apply(df, "Carapace_L", hlp.recode_decimal, downcast=True)
    df = hlp.copy_value_and_apply(df, "Plastron_L", hlp.recode_decimal, downcast=True)
    df = hlp.copy_value_and_apply(df, "Plastron_S", hlp.recode_decimal, downcast=True)

    return df


def new_features(df):
    """ Add new features to data """
    print("Add new features ...")
    # distinguish Spring, Fall and pregnant females (don't care about juvenilles/unknown)
    df["gender_plus"] = df["Gender"]
    df.loc[df.Gravid, "gender_plus"] = "f_gra"

    df["gender_seasons"] = df["Gender"]
    df.loc[df.Gravid, "gender_seasons"] = "f_gra"

    # add features
    df["Age_To_Weight"] = df["Annuli"] / df["Weight"]

    # Calcuate Number of recaptures
    df_captures = df[["ID", "Date"]].groupby("ID").count()
    df_captures.columns = ["recapture_count"]
    df_captures.reset_index(inplace=True)
    df = pd.merge(df, df_captures, how="outer", on="ID")

    # recalculate annuli
    df_min = pd.pivot_table(
        df[df.Annuli > 0],
        values=["Date", "Annuli"],
        index=["ID"],
        aggfunc={"Date": min, "Annuli": min},
    )
    df_min.columns = ["annuli_min", "date_min"]
    df_min.reset_index(inplace=True)

    df = pd.merge(df, df_min, how="outer", on="ID")
    df["year"] = df.Date.map(lambda x: x.year)
    df["year_min"] = df.date_min.map(lambda x: x.year)
    df["Annuli_orig"] = df.Annuli
    df.Annuli = df.year - df.year_min + df.annuli_min
    df.Annuli = np.nan_to_num(df.Annuli)
    df["Annuli"] = pd.to_numeric(df["Annuli"], downcast="integer")

    # Annuli Buckets
    buckets = 5
    interval = int(df["Annuli"].max() / buckets)
    buckets = [i for i in range(0, df["Annuli"].max() + interval, interval)]
    labels = ["'{0} - {1}'".format(i, i + interval) for i in buckets]
    df["Annuli_Group"] = pd.cut(
        df.Annuli, buckets, labels=labels[:-1], include_lowest=True
    )

    return df


if __name__ == "__main__":
    print("Loading, cleaning and transforming data...")
    df = get_clean_data()
    csvPath = TARGET_PATH + FILE_NAME_CLEAN + ".csv"
    print("Save as csv...")
    df.to_csv(csvPath)

    gc = gspread.oauth()
    # content = open(csvPath, "r").read()
    # print("Import cleanded data to gsheets...")
    # spreadsheet = gc.open(FILE_NAME_CLEAN)
    # gc.import_csv(spreadsheet.id, content.encode("utf-8"))

    print("Sites...")
    siteDf = site_data(df)
    csvPath = TARGET_PATH + "sites.csv"
    siteDf.to_csv(csvPath)
    content = open(csvPath, "r").read()
    print("Import cleanded data to gsheets...")
    spreadsheet = gc.open("sites")
    gc.import_csv(spreadsheet.id, content.encode("utf-8"))
