import argparse
import os
import traceback
import pandas as pd
import numpy as np
import json
import API.M6 as M6


mapping = pd.read_csv("/home/infra/user/ya/행정구역코드.csv",header=None)
mapping.columns = ["a","b"]
mapping["a"]=mapping["a"].astype("str")
mapping["b"]=mapping["b"].astype("str")

def pop_data(cursor,info,std_year):
    sql = f"SELECT * FROM SUWON_ADMINISTRATION_ZONE_AGE_ACCORDING_TO_POPULATION WHERE STANDARD_YEAR>={std_year-6} AND STANDARD_YEAR<{std_year};"
    cursor.Execute2(sql)
    df = pd.DataFrame(cursor)
    df.drop([df.columns[0],df.columns[1]],axis=1,inplace=True)
    headers = info['header']['pop']
    df.columns = headers
    df.to_csv('/home/infra/user/ya/param/pop_data.csv',index=False,mode="w",encoding='utf-8-sig')

def movein_pop_data(cursor,info,std_year):
    sql = f"SELECT * FROM SUWON_CITY_TRANSFERENCE_POPULATION_COUNT WHERE TRANSFERENCE_YEAR>={std_year-6} AND TRANSFERENCE_YEAR<{std_year};"
    cursor.Execute2(sql)
    df = pd.DataFrame(cursor)
    df.drop([df.columns[0],df.columns[1]],axis=1,inplace=True)
    df.to_csv('/home/infra/user/ya/param/movein_pop_data.csv',index=False,mode="w",encoding='utf-8-sig')
def moveout_pop_data(cursor,info,std_year):
    sql = f"SELECT * FROM SUWON_CITY_MOVING_OUT_POPULATION_COUNT WHERE TRANSFERENCE_YEAR>={std_year-6} AND TRANSFERENCE_YEAR<{std_year};"
    cursor.Execute2(sql)
    df = pd.DataFrame(cursor)
    df.drop([df.columns[0],df.columns[1]],axis=1,inplace=True)
    df.to_csv('/home/infra/user/ya/param/moveout_pop_data.csv',index=False,mode="w",encoding='utf-8-sig')

def bd_pop_data(cursor,info,std_year):
    sql = f"SELECT * FROM SUWON_CITY_BIRTH_DEATH_POPULATION_COUNT WHERE POINT_TIME>={std_year-6} AND POINT_TIME<{std_year};"
    cursor.Execute2(sql)
    df = pd.DataFrame(cursor)
    df.drop([df.columns[0],df.columns[1]],axis=1,inplace=True)
    headers = info['header']['bd_pop']
    df.columns = headers
    df.to_csv('/home/infra/user/ya/param/bd_pop_data.csv',index=False,mode="w",encoding='utf-8-sig')

def death_pop_data(cursor,info,std_year):
    sql = f"SELECT * FROM SUWON_CITY_AGE_ADMINISTRATION_ZONE_ACCORDING_TO_DEATH_POPULATION_COUNT WHERE POINT_TIME>={std_year-6} AND POINT_TIME<{std_year};"
    cursor.Execute2(sql)
    df = pd.DataFrame(cursor)
    df.drop([df.columns[0],df.columns[1]],axis=1,inplace=True)
    headers = info['header']['death_pop']
    df.columns = headers
    df.to_csv('/home/infra/user/ya/param/death_pop_data.csv',index=False,mode="w",encoding='utf-8-sig')

def movein_preprocess(data):
    data.columns = ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10",
                  "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20",
                  "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "V29", "V30",
                  "V31", "V32", "V33", "V34", "V35", "V36", "V37", "V38", "V39", "V40", "V41"]

    data["V1"] = data["V1"].astype("str")
    data["V7"] = data["V7"].astype("str")
    data["V2"] = data["V1"].map(str) + data["V2"].map(str)
    data["V3"] = data["V2"].map(str) + data["V3"].map(str)
    data["V8"] = data["V7"].map(str) + data["V8"].map(str)
    data["V9"] = data["V8"].map(str) + data["V9"].map(str)
    data = pd.merge(data, mapping, left_on = "V1",right_on="a",how='left')
    data.loc[:,"V1"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V2",right_on="a",how='left')
    data.loc[:,"V2"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    test = pd.merge(data, mapping, left_on = "V3",right_on="a",how='left')
    test.to_csv('/home/infra/user/ya/param/test.csv',index=False,mode="w",encoding='utf-8-sig')
    data = pd.merge(data, mapping, left_on = "V3",right_on="a",how='left')
    data.loc[:,"V3"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V7",right_on="a",how='left')
    data.loc[:,"V7"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V8",right_on="a",how='left')
    data.loc[:,"V8"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V9",right_on="a",how='left')
    data.loc[:,"V9"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)

    data.loc[data["V13"]==0,"V13"] = "여자"
    data.loc[data["V13"]==1,"V13"] = "남자"
    data.loc[data["V13"]==2,"V13"] = "여자"
    data.loc[data["V13"]==3,"V13"] = "남자"
    data.loc[data["V13"]==4,"V13"] = "여자"
    data.loc[data["V13"]==9,"V13"] = "남자"
    data.loc[data["V16"]==0,"V16"] = "여자"
    data.loc[data["V16"]==1,"V16"] = "남자"
    data.loc[data["V16"]==2,"V16"] = "여자"
    data.loc[data["V16"]==3,"V16"] = "남자"
    data.loc[data["V16"]==4,"V16"] = "여자"
    data.loc[data["V16"]==9,"V16"] = "남자"
    data.loc[data["V19"]==0,"V19"] = "여자"
    data.loc[data["V19"]==1,"V19"] = "남자"
    data.loc[data["V19"]==2,"V19"] = "여자"
    data.loc[data["V19"]==3,"V19"] = "남자"
    data.loc[data["V19"]==4,"V19"] = "여자"
    data.loc[data["V19"]==9,"V19"] = "남자"
    data.loc[data["V22"]==0,"V22"] = "여자"
    data.loc[data["V22"]==1,"V22"] = "남자"
    data.loc[data["V22"]==2,"V22"] = "여자"
    data.loc[data["V22"]==3,"V22"] = "남자"
    data.loc[data["V22"]==4,"V22"] = "여자"
    data.loc[data["V22"]==9,"V22"] = "남자"
    data.loc[data["V25"]==0,"V25"] = "여자"
    data.loc[data["V25"]==1,"V25"] = "남자"
    data.loc[data["V25"]==2,"V25"] = "여자"
    data.loc[data["V25"]==3,"V25"] = "남자"
    data.loc[data["V25"]==4,"V25"] = "여자"
    data.loc[data["V25"]==9,"V25"] = "남자"
    data.loc[data["V28"]==0,"V25"] = "여자"
    data.loc[data["V29"]==1,"V25"] = "남자"
    data.loc[data["V28"]==2,"V28"] = "여자"
    data.loc[data["V28"]==3,"V28"] = "남자"
    data.loc[data["V28"]==4,"V28"] = "여자"
    data.loc[data["V28"]==9,"V28"] = "남자"
    data.loc[data["V31"]==0,"V31"] = "여자"
    data.loc[data["V31"]==1,"V31"] = "남자"
    data.loc[data["V31"]==2,"V31"] = "여자"
    data.loc[data["V31"]==3,"V31"] = "남자"
    data.loc[data["V31"]==4,"V31"] = "여자"
    data.loc[data["V31"]==9,"V31"] = "남자"
    data.loc[data["V34"]==0,"V34"] = "여자"
    data.loc[data["V34"]==1,"V34"] = "남자"
    data.loc[data["V34"]==2,"V34"] = "여자"
    data.loc[data["V34"]==3,"V34"] = "남자"
    data.loc[data["V34"]==4,"V34"] = "여자"
    data.loc[data["V34"]==9,"V34"] = "남자"
    data.loc[data["V37"]==0,"V37"] = "여자"
    data.loc[data["V37"]==1,"V37"] = "남자"
    data.loc[data["V37"]==2,"V37"] = "여자"
    data.loc[data["V37"]==3,"V37"] = "남자"
    data.loc[data["V37"]==4,"V37"] = "여자"
    data.loc[data["V37"]==9,"V37"] = "남자"
    data.loc[data["V40"]==0,"V40"] = "여자"
    data.loc[data["V40"]==1,"V40"] = "남자"
    data.loc[data["V40"]==2,"V40"] = "여자"
    data.loc[data["V40"]==3,"V40"] = "남자"
    data.loc[data["V40"]==4,"V40"] = "여자"
    data.loc[data["V40"]==9,"V40"] = "남자"

    data.columns = ["전입행정_시도", "전입행정_시군구", "전입행정_동읍면", "전입년", "전입월", "전입일",
                  "전출행정_시도", "전출행정_시군구", "전출행정_동읍면", "전입사유", "전입자1_관계",
                  "전입자1_만나이", "전입자1_성별", "전입자2_관계", "전입자2_만나이", "전입자2_성별",
                  "전입자3_관계", "전입자3_만나이", "전입자3_성별", "전입자4_관계", "전입자4_만나이",
                  "전입자4_성별", "전입자5_관계", "전입자5_만나이", "전입자5_성별", "전입자6_관계",
                  "전입자6_만나이", "전입자6_성별", "전입자7_관계", "전입자7_만나이", "전입자7_성별",
                  "전입자8_관계", "전입자8_만나이", "전입자8_성별", "전입자9_관계", "전입자9_만나이",
                  "전입자9_성별", "전입자10_관계", "전입자10_만나이", "전입자10_성별", "일련번호"]
    data.to_csv('/home/infra/user/ya/param/movein.csv',mode="w")

def age_factor_in(data) :
    data.loc[data["전입자1_만나이"] <5,"전입자1_나이"] = "0-4세"
    data.loc[(data["전입자1_만나이"] >= 5) & (data["전입자1_만나이"] < 10),"전입자1_나이"] = "5 - 9세"
    data.loc[(data["전입자1_만나이"] >= 10) & (data["전입자1_만나이"] < 15),"전입자1_나이"] = "10 - 14세"
    data.loc[(data["전입자1_만나이"] >= 15) & (data["전입자1_만나이"] < 20),"전입자1_나이"] = "15 - 19세"
    data.loc[(data["전입자1_만나이"] >= 20) & (data["전입자1_만나이"] < 25),"전입자1_나이"] = "20 - 24세"
    data.loc[(data["전입자1_만나이"] >= 25) & (data["전입자1_만나이"] < 30),"전입자1_나이"] = "25 - 29세"
    data.loc[(data["전입자1_만나이"] >= 30) & (data["전입자1_만나이"] < 35),"전입자1_나이"] = "30 - 34세"
    data.loc[(data["전입자1_만나이"] >= 35) & (data["전입자1_만나이"] < 40),"전입자1_나이"] = "35 - 39세"
    data.loc[(data["전입자1_만나이"] >= 40) & (data["전입자1_만나이"] < 45),"전입자1_나이"] = "40 - 44세"
    data.loc[(data["전입자1_만나이"] >= 45) & (data["전입자1_만나이"] < 50),"전입자1_나이"] = "45 - 49세"
    data.loc[(data["전입자1_만나이"] >= 50) & (data["전입자1_만나이"] < 55),"전입자1_나이"] = "50 - 54세"
    data.loc[(data["전입자1_만나이"] >= 55) & (data["전입자1_만나이"] < 60),"전입자1_나이"] = "55 - 59세"
    data.loc[(data["전입자1_만나이"] >= 60) & (data["전입자1_만나이"] < 65),"전입자1_나이"] = "60 - 64세"
    data.loc[(data["전입자1_만나이"] >= 65) & (data["전입자1_만나이"] < 70),"전입자1_나이"] = "65 - 69세"
    data.loc[(data["전입자1_만나이"] >= 70) & (data["전입자1_만나이"] < 75),"전입자1_나이"] = "70 - 74세"
    data.loc[(data["전입자1_만나이"] >= 75) & (data["전입자1_만나이"] < 80),"전입자1_나이"] = "75 - 79세"
    data.loc[(data["전입자1_만나이"] >= 80) & (data["전입자1_만나이"] < 85),"전입자1_나이"] = "80 - 84세"
    data.loc[(data["전입자1_만나이"] >= 85) & (data["전입자1_만나이"] < 90),"전입자1_나이"] = "85 - 89세"
    data.loc[(data["전입자1_만나이"] >= 90),"전입자1_나이"] = "90세 이상"

    data["전입자1_나이"] = pd.Categorical(data["전입자1_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자2_만나이"] <5,"전입자2_나이"] = "0-4세"
    data.loc[(data["전입자2_만나이"] >= 5) & (data["전입자2_만나이"] < 10),"전입자2_나이"] = "5 - 9세"
    data.loc[(data["전입자2_만나이"] >= 10) & (data["전입자2_만나이"] < 15),"전입자2_나이"] = "10 - 14세"
    data.loc[(data["전입자2_만나이"] >= 15) & (data["전입자2_만나이"] < 20),"전입자2_나이"] = "15 - 19세"
    data.loc[(data["전입자2_만나이"] >= 20) & (data["전입자2_만나이"] < 25),"전입자2_나이"] = "20 - 24세"
    data.loc[(data["전입자2_만나이"] >= 25) & (data["전입자2_만나이"] < 30),"전입자2_나이"] = "25 - 29세"
    data.loc[(data["전입자2_만나이"] >= 30) & (data["전입자2_만나이"] < 35),"전입자2_나이"] = "30 - 34세"
    data.loc[(data["전입자2_만나이"] >= 35) & (data["전입자2_만나이"] < 40),"전입자2_나이"] = "35 - 39세"
    data.loc[(data["전입자2_만나이"] >= 40) & (data["전입자2_만나이"] < 45),"전입자2_나이"] = "40 - 44세"
    data.loc[(data["전입자2_만나이"] >= 45) & (data["전입자2_만나이"] < 50),"전입자2_나이"] = "45 - 49세"
    data.loc[(data["전입자2_만나이"] >= 50) & (data["전입자2_만나이"] < 55),"전입자2_나이"] = "50 - 54세"
    data.loc[(data["전입자2_만나이"] >= 55) & (data["전입자2_만나이"] < 60),"전입자2_나이"] = "55 - 59세"
    data.loc[(data["전입자2_만나이"] >= 60) & (data["전입자2_만나이"] < 65),"전입자2_나이"] = "60 - 64세"
    data.loc[(data["전입자2_만나이"] >= 65) & (data["전입자2_만나이"] < 70),"전입자2_나이"] = "65 - 69세"
    data.loc[(data["전입자2_만나이"] >= 70) & (data["전입자2_만나이"] < 75),"전입자2_나이"] = "70 - 74세"
    data.loc[(data["전입자2_만나이"] >= 75) & (data["전입자2_만나이"] < 80),"전입자2_나이"] = "75 - 79세"
    data.loc[(data["전입자2_만나이"] >= 80) & (data["전입자2_만나이"] < 85),"전입자2_나이"] = "80 - 84세"
    data.loc[(data["전입자2_만나이"] >= 85) & (data["전입자2_만나이"] < 90),"전입자2_나이"] = "85 - 89세"
    data.loc[(data["전입자2_만나이"] >= 90),"전입자2_나이"] = "90세 이상"

    data["전입자2_나이"] = pd.Categorical(data["전입자2_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자3_만나이"] <5,"전입자3_나이"] = "0-4세"
    data.loc[(data["전입자3_만나이"] >= 5) & (data["전입자3_만나이"] < 10),"전입자3_나이"] = "5 - 9세"
    data.loc[(data["전입자3_만나이"] >= 10) & (data["전입자3_만나이"] < 15),"전입자3_나이"] = "10 - 14세"
    data.loc[(data["전입자3_만나이"] >= 15) & (data["전입자3_만나이"] < 20),"전입자3_나이"] = "15 - 19세"
    data.loc[(data["전입자3_만나이"] >= 20) & (data["전입자3_만나이"] < 25),"전입자3_나이"] = "20 - 24세"
    data.loc[(data["전입자3_만나이"] >= 25) & (data["전입자3_만나이"] < 30),"전입자3_나이"] = "25 - 29세"
    data.loc[(data["전입자3_만나이"] >= 30) & (data["전입자3_만나이"] < 35),"전입자3_나이"] = "30 - 34세"
    data.loc[(data["전입자3_만나이"] >= 35) & (data["전입자3_만나이"] < 40),"전입자3_나이"] = "35 - 39세"
    data.loc[(data["전입자3_만나이"] >= 40) & (data["전입자3_만나이"] < 45),"전입자3_나이"] = "40 - 44세"
    data.loc[(data["전입자3_만나이"] >= 45) & (data["전입자3_만나이"] < 50),"전입자3_나이"] = "45 - 49세"
    data.loc[(data["전입자3_만나이"] >= 50) & (data["전입자3_만나이"] < 55),"전입자3_나이"] = "50 - 54세"
    data.loc[(data["전입자3_만나이"] >= 55) & (data["전입자3_만나이"] < 60),"전입자3_나이"] = "55 - 59세"
    data.loc[(data["전입자3_만나이"] >= 60) & (data["전입자3_만나이"] < 65),"전입자3_나이"] = "60 - 64세"
    data.loc[(data["전입자3_만나이"] >= 65) & (data["전입자3_만나이"] < 70),"전입자3_나이"] = "65 - 69세"
    data.loc[(data["전입자3_만나이"] >= 70) & (data["전입자3_만나이"] < 75),"전입자3_나이"] = "70 - 74세"
    data.loc[(data["전입자3_만나이"] >= 75) & (data["전입자3_만나이"] < 80),"전입자3_나이"] = "75 - 79세"
    data.loc[(data["전입자3_만나이"] >= 80) & (data["전입자3_만나이"] < 85),"전입자3_나이"] = "80 - 84세"
    data.loc[(data["전입자3_만나이"] >= 85) & (data["전입자3_만나이"] < 90),"전입자3_나이"] = "85 - 89세"
    data.loc[(data["전입자3_만나이"] >= 90),"전입자3_나이"] = "90세 이상"

    data["전입자3_나이"] = pd.Categorical(data["전입자3_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자4_만나이"] <5,"전입자4_나이"] = "0-4세"
    data.loc[(data["전입자4_만나이"] >= 5) & (data["전입자4_만나이"] < 10),"전입자4_나이"] = "5 - 9세"
    data.loc[(data["전입자4_만나이"] >= 10) & (data["전입자4_만나이"] < 15),"전입자4_나이"] = "10 - 14세"
    data.loc[(data["전입자4_만나이"] >= 15) & (data["전입자4_만나이"] < 20),"전입자4_나이"] = "15 - 19세"
    data.loc[(data["전입자4_만나이"] >= 20) & (data["전입자4_만나이"] < 25),"전입자4_나이"] = "20 - 24세"
    data.loc[(data["전입자4_만나이"] >= 25) & (data["전입자4_만나이"] < 30),"전입자4_나이"] = "25 - 29세"
    data.loc[(data["전입자4_만나이"] >= 30) & (data["전입자4_만나이"] < 35),"전입자4_나이"] = "30 - 34세"
    data.loc[(data["전입자4_만나이"] >= 35) & (data["전입자4_만나이"] < 40),"전입자4_나이"] = "35 - 39세"
    data.loc[(data["전입자4_만나이"] >= 40) & (data["전입자4_만나이"] < 45),"전입자4_나이"] = "40 - 44세"
    data.loc[(data["전입자4_만나이"] >= 45) & (data["전입자4_만나이"] < 50),"전입자4_나이"] = "45 - 49세"
    data.loc[(data["전입자4_만나이"] >= 50) & (data["전입자4_만나이"] < 55),"전입자4_나이"] = "50 - 54세"
    data.loc[(data["전입자4_만나이"] >= 55) & (data["전입자4_만나이"] < 60),"전입자4_나이"] = "55 - 59세"
    data.loc[(data["전입자4_만나이"] >= 60) & (data["전입자4_만나이"] < 65),"전입자4_나이"] = "60 - 64세"
    data.loc[(data["전입자4_만나이"] >= 65) & (data["전입자4_만나이"] < 70),"전입자4_나이"] = "65 - 69세"
    data.loc[(data["전입자4_만나이"] >= 70) & (data["전입자4_만나이"] < 75),"전입자4_나이"] = "70 - 74세"
    data.loc[(data["전입자4_만나이"] >= 75) & (data["전입자4_만나이"] < 80),"전입자4_나이"] = "75 - 79세"
    data.loc[(data["전입자4_만나이"] >= 80) & (data["전입자4_만나이"] < 85),"전입자4_나이"] = "80 - 84세"
    data.loc[(data["전입자4_만나이"] >= 85) & (data["전입자4_만나이"] < 90),"전입자4_나이"] = "85 - 89세"
    data.loc[(data["전입자4_만나이"] >= 90),"전입자4_나이"] = "90세 이상"

    data["전입자4_나이"] = pd.Categorical(data["전입자4_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자5_만나이"] <5,"전입자5_나이"] = "0-4세"
    data.loc[(data["전입자5_만나이"] >= 5) & (data["전입자5_만나이"] < 10),"전입자5_나이"] = "5 - 9세"
    data.loc[(data["전입자5_만나이"] >= 10) & (data["전입자5_만나이"] < 15),"전입자5_나이"] = "10 - 14세"
    data.loc[(data["전입자5_만나이"] >= 15) & (data["전입자5_만나이"] < 20),"전입자5_나이"] = "15 - 19세"
    data.loc[(data["전입자5_만나이"] >= 20) & (data["전입자5_만나이"] < 25),"전입자5_나이"] = "20 - 24세"
    data.loc[(data["전입자5_만나이"] >= 25) & (data["전입자5_만나이"] < 30),"전입자5_나이"] = "25 - 29세"
    data.loc[(data["전입자5_만나이"] >= 30) & (data["전입자5_만나이"] < 35),"전입자5_나이"] = "30 - 34세"
    data.loc[(data["전입자5_만나이"] >= 35) & (data["전입자5_만나이"] < 40),"전입자5_나이"] = "35 - 39세"
    data.loc[(data["전입자5_만나이"] >= 40) & (data["전입자5_만나이"] < 45),"전입자5_나이"] = "40 - 44세"
    data.loc[(data["전입자5_만나이"] >= 45) & (data["전입자5_만나이"] < 50),"전입자5_나이"] = "45 - 49세"
    data.loc[(data["전입자5_만나이"] >= 50) & (data["전입자5_만나이"] < 55),"전입자5_나이"] = "50 - 54세"
    data.loc[(data["전입자5_만나이"] >= 55) & (data["전입자5_만나이"] < 60),"전입자5_나이"] = "55 - 59세"
    data.loc[(data["전입자5_만나이"] >= 60) & (data["전입자5_만나이"] < 65),"전입자5_나이"] = "60 - 64세"
    data.loc[(data["전입자5_만나이"] >= 65) & (data["전입자5_만나이"] < 70),"전입자5_나이"] = "65 - 69세"
    data.loc[(data["전입자5_만나이"] >= 70) & (data["전입자5_만나이"] < 75),"전입자5_나이"] = "70 - 74세"
    data.loc[(data["전입자5_만나이"] >= 75) & (data["전입자5_만나이"] < 80),"전입자5_나이"] = "75 - 79세"
    data.loc[(data["전입자5_만나이"] >= 80) & (data["전입자5_만나이"] < 85),"전입자5_나이"] = "80 - 84세"
    data.loc[(data["전입자5_만나이"] >= 85) & (data["전입자5_만나이"] < 90),"전입자5_나이"] = "85 - 89세"
    data.loc[(data["전입자5_만나이"] >= 90),"전입자5_나이"] = "90세 이상"

    data["전입자5_나이"] = pd.Categorical(data["전입자5_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자6_만나이"] <5,"전입자6_나이"] = "0-4세"
    data.loc[(data["전입자6_만나이"] >= 5) & (data["전입자6_만나이"] < 10),"전입자6_나이"] = "5 - 9세"
    data.loc[(data["전입자6_만나이"] >= 10) & (data["전입자6_만나이"] < 15),"전입자6_나이"] = "10 - 14세"
    data.loc[(data["전입자6_만나이"] >= 15) & (data["전입자6_만나이"] < 20),"전입자6_나이"] = "15 - 19세"
    data.loc[(data["전입자6_만나이"] >= 20) & (data["전입자6_만나이"] < 25),"전입자6_나이"] = "20 - 24세"
    data.loc[(data["전입자6_만나이"] >= 25) & (data["전입자6_만나이"] < 30),"전입자6_나이"] = "25 - 29세"
    data.loc[(data["전입자6_만나이"] >= 30) & (data["전입자6_만나이"] < 35),"전입자6_나이"] = "30 - 34세"
    data.loc[(data["전입자6_만나이"] >= 35) & (data["전입자6_만나이"] < 40),"전입자6_나이"] = "35 - 39세"
    data.loc[(data["전입자6_만나이"] >= 40) & (data["전입자6_만나이"] < 45),"전입자6_나이"] = "40 - 44세"
    data.loc[(data["전입자6_만나이"] >= 45) & (data["전입자6_만나이"] < 50),"전입자6_나이"] = "45 - 49세"
    data.loc[(data["전입자6_만나이"] >= 50) & (data["전입자6_만나이"] < 55),"전입자6_나이"] = "50 - 54세"
    data.loc[(data["전입자6_만나이"] >= 55) & (data["전입자6_만나이"] < 60),"전입자6_나이"] = "55 - 59세"
    data.loc[(data["전입자6_만나이"] >= 60) & (data["전입자6_만나이"] < 65),"전입자6_나이"] = "60 - 64세"
    data.loc[(data["전입자6_만나이"] >= 65) & (data["전입자6_만나이"] < 70),"전입자6_나이"] = "65 - 69세"
    data.loc[(data["전입자6_만나이"] >= 70) & (data["전입자6_만나이"] < 75),"전입자6_나이"] = "70 - 74세"
    data.loc[(data["전입자6_만나이"] >= 75) & (data["전입자6_만나이"] < 80),"전입자6_나이"] = "75 - 79세"
    data.loc[(data["전입자6_만나이"] >= 80) & (data["전입자6_만나이"] < 85),"전입자6_나이"] = "80 - 84세"
    data.loc[(data["전입자6_만나이"] >= 85) & (data["전입자6_만나이"] < 90),"전입자6_나이"] = "85 - 89세"
    data.loc[(data["전입자6_만나이"] >= 90),"전입자6_나이"] = "90세 이상"

    data["전입자6_나이"] = pd.Categorical(data["전입자6_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자7_만나이"] <5,"전입자7_나이"] = "0-4세"
    data.loc[(data["전입자7_만나이"] >= 5) & (data["전입자7_만나이"] < 10),"전입자7_나이"] = "5 - 9세"
    data.loc[(data["전입자7_만나이"] >= 10) & (data["전입자7_만나이"] < 15),"전입자7_나이"] = "10 - 14세"
    data.loc[(data["전입자7_만나이"] >= 15) & (data["전입자7_만나이"] < 20),"전입자7_나이"] = "15 - 19세"
    data.loc[(data["전입자7_만나이"] >= 20) & (data["전입자7_만나이"] < 25),"전입자7_나이"] = "20 - 24세"
    data.loc[(data["전입자7_만나이"] >= 25) & (data["전입자7_만나이"] < 30),"전입자7_나이"] = "25 - 29세"
    data.loc[(data["전입자7_만나이"] >= 30) & (data["전입자7_만나이"] < 35),"전입자7_나이"] = "30 - 34세"
    data.loc[(data["전입자7_만나이"] >= 35) & (data["전입자7_만나이"] < 40),"전입자7_나이"] = "35 - 39세"
    data.loc[(data["전입자7_만나이"] >= 40) & (data["전입자7_만나이"] < 45),"전입자7_나이"] = "40 - 44세"
    data.loc[(data["전입자7_만나이"] >= 45) & (data["전입자7_만나이"] < 50),"전입자7_나이"] = "45 - 49세"
    data.loc[(data["전입자7_만나이"] >= 50) & (data["전입자7_만나이"] < 55),"전입자7_나이"] = "50 - 54세"
    data.loc[(data["전입자7_만나이"] >= 55) & (data["전입자7_만나이"] < 60),"전입자7_나이"] = "55 - 59세"
    data.loc[(data["전입자7_만나이"] >= 60) & (data["전입자7_만나이"] < 65),"전입자7_나이"] = "60 - 64세"
    data.loc[(data["전입자7_만나이"] >= 65) & (data["전입자7_만나이"] < 70),"전입자7_나이"] = "65 - 69세"
    data.loc[(data["전입자7_만나이"] >= 70) & (data["전입자7_만나이"] < 75),"전입자7_나이"] = "70 - 74세"
    data.loc[(data["전입자7_만나이"] >= 75) & (data["전입자7_만나이"] < 80),"전입자7_나이"] = "75 - 79세"
    data.loc[(data["전입자7_만나이"] >= 80) & (data["전입자7_만나이"] < 85),"전입자7_나이"] = "80 - 84세"
    data.loc[(data["전입자7_만나이"] >= 85) & (data["전입자7_만나이"] < 90),"전입자7_나이"] = "85 - 89세"
    data.loc[(data["전입자7_만나이"] >= 90),"전입자7_나이"] = "90세 이상"

    data["전입자7_나이"] = pd.Categorical(data["전입자7_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자8_만나이"] <5,"전입자8_나이"] = "0-4세"
    data.loc[(data["전입자8_만나이"] >= 5) & (data["전입자8_만나이"] < 10),"전입자8_나이"] = "5 - 9세"
    data.loc[(data["전입자8_만나이"] >= 10) & (data["전입자8_만나이"] < 15),"전입자8_나이"] = "10 - 14세"
    data.loc[(data["전입자8_만나이"] >= 15) & (data["전입자8_만나이"] < 20),"전입자8_나이"] = "15 - 19세"
    data.loc[(data["전입자8_만나이"] >= 20) & (data["전입자8_만나이"] < 25),"전입자8_나이"] = "20 - 24세"
    data.loc[(data["전입자8_만나이"] >= 25) & (data["전입자8_만나이"] < 30),"전입자8_나이"] = "25 - 29세"
    data.loc[(data["전입자8_만나이"] >= 30) & (data["전입자8_만나이"] < 35),"전입자8_나이"] = "30 - 34세"
    data.loc[(data["전입자8_만나이"] >= 35) & (data["전입자8_만나이"] < 40),"전입자8_나이"] = "35 - 39세"
    data.loc[(data["전입자8_만나이"] >= 40) & (data["전입자8_만나이"] < 45),"전입자8_나이"] = "40 - 44세"
    data.loc[(data["전입자8_만나이"] >= 45) & (data["전입자8_만나이"] < 50),"전입자8_나이"] = "45 - 49세"
    data.loc[(data["전입자8_만나이"] >= 50) & (data["전입자8_만나이"] < 55),"전입자8_나이"] = "50 - 54세"
    data.loc[(data["전입자8_만나이"] >= 55) & (data["전입자8_만나이"] < 60),"전입자8_나이"] = "55 - 59세"
    data.loc[(data["전입자8_만나이"] >= 60) & (data["전입자8_만나이"] < 65),"전입자8_나이"] = "60 - 64세"
    data.loc[(data["전입자8_만나이"] >= 65) & (data["전입자8_만나이"] < 70),"전입자8_나이"] = "65 - 69세"
    data.loc[(data["전입자8_만나이"] >= 70) & (data["전입자8_만나이"] < 75),"전입자8_나이"] = "70 - 74세"
    data.loc[(data["전입자8_만나이"] >= 75) & (data["전입자8_만나이"] < 80),"전입자8_나이"] = "75 - 79세"
    data.loc[(data["전입자8_만나이"] >= 80) & (data["전입자8_만나이"] < 85),"전입자8_나이"] = "80 - 84세"
    data.loc[(data["전입자8_만나이"] >= 85) & (data["전입자8_만나이"] < 90),"전입자8_나이"] = "85 - 89세"
    data.loc[(data["전입자8_만나이"] >= 90),"전입자8_나이"] = "90세 이상"

    data["전입자8_나이"] = pd.Categorical(data["전입자8_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자9_만나이"] <5,"전입자9_나이"] = "0-4세"
    data.loc[(data["전입자9_만나이"] >= 5) & (data["전입자9_만나이"] < 10),"전입자9_나이"] = "5 - 9세"
    data.loc[(data["전입자9_만나이"] >= 10) & (data["전입자9_만나이"] < 15),"전입자9_나이"] = "10 - 14세"
    data.loc[(data["전입자9_만나이"] >= 15) & (data["전입자9_만나이"] < 20),"전입자9_나이"] = "15 - 19세"
    data.loc[(data["전입자9_만나이"] >= 20) & (data["전입자9_만나이"] < 25),"전입자9_나이"] = "20 - 24세"
    data.loc[(data["전입자9_만나이"] >= 25) & (data["전입자9_만나이"] < 30),"전입자9_나이"] = "25 - 29세"
    data.loc[(data["전입자9_만나이"] >= 30) & (data["전입자9_만나이"] < 35),"전입자9_나이"] = "30 - 34세"
    data.loc[(data["전입자9_만나이"] >= 35) & (data["전입자9_만나이"] < 40),"전입자9_나이"] = "35 - 39세"
    data.loc[(data["전입자9_만나이"] >= 40) & (data["전입자9_만나이"] < 45),"전입자9_나이"] = "40 - 44세"
    data.loc[(data["전입자9_만나이"] >= 45) & (data["전입자9_만나이"] < 50),"전입자9_나이"] = "45 - 49세"
    data.loc[(data["전입자9_만나이"] >= 50) & (data["전입자9_만나이"] < 55),"전입자9_나이"] = "50 - 54세"
    data.loc[(data["전입자9_만나이"] >= 55) & (data["전입자9_만나이"] < 60),"전입자9_나이"] = "55 - 59세"
    data.loc[(data["전입자9_만나이"] >= 60) & (data["전입자9_만나이"] < 65),"전입자9_나이"] = "60 - 64세"
    data.loc[(data["전입자9_만나이"] >= 65) & (data["전입자9_만나이"] < 70),"전입자9_나이"] = "65 - 69세"
    data.loc[(data["전입자9_만나이"] >= 70) & (data["전입자9_만나이"] < 75),"전입자9_나이"] = "70 - 74세"
    data.loc[(data["전입자9_만나이"] >= 75) & (data["전입자9_만나이"] < 80),"전입자9_나이"] = "75 - 79세"
    data.loc[(data["전입자9_만나이"] >= 80) & (data["전입자9_만나이"] < 85),"전입자9_나이"] = "80 - 84세"
    data.loc[(data["전입자9_만나이"] >= 85) & (data["전입자9_만나이"] < 90),"전입자9_나이"] = "85 - 89세"
    data.loc[(data["전입자9_만나이"] >= 90),"전입자9_나이"] = "90세 이상"

    data["전입자9_나이"] = pd.Categorical(data["전입자9_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자10_만나이"] <5,"전입자10_나이"] = "0-4세"
    data.loc[(data["전입자10_만나이"] >= 5) & (data["전입자10_만나이"] < 10),"전입자10_나이"] = "5 - 9세"
    data.loc[(data["전입자10_만나이"] >= 10) & (data["전입자10_만나이"] < 15),"전입자10_나이"] = "10 - 14세"
    data.loc[(data["전입자10_만나이"] >= 15) & (data["전입자10_만나이"] < 20),"전입자10_나이"] = "15 - 19세"
    data.loc[(data["전입자10_만나이"] >= 20) & (data["전입자10_만나이"] < 25),"전입자10_나이"] = "20 - 24세"
    data.loc[(data["전입자10_만나이"] >= 25) & (data["전입자10_만나이"] < 30),"전입자10_나이"] = "25 - 29세"
    data.loc[(data["전입자10_만나이"] >= 30) & (data["전입자10_만나이"] < 35),"전입자10_나이"] = "30 - 34세"
    data.loc[(data["전입자10_만나이"] >= 35) & (data["전입자10_만나이"] < 40),"전입자10_나이"] = "35 - 39세"
    data.loc[(data["전입자10_만나이"] >= 40) & (data["전입자10_만나이"] < 45),"전입자10_나이"] = "40 - 44세"
    data.loc[(data["전입자10_만나이"] >= 45) & (data["전입자10_만나이"] < 50),"전입자10_나이"] = "45 - 49세"
    data.loc[(data["전입자10_만나이"] >= 50) & (data["전입자10_만나이"] < 55),"전입자10_나이"] = "50 - 54세"
    data.loc[(data["전입자10_만나이"] >= 55) & (data["전입자10_만나이"] < 60),"전입자10_나이"] = "55 - 59세"
    data.loc[(data["전입자10_만나이"] >= 60) & (data["전입자10_만나이"] < 65),"전입자10_나이"] = "60 - 64세"
    data.loc[(data["전입자10_만나이"] >= 65) & (data["전입자10_만나이"] < 70),"전입자10_나이"] = "65 - 69세"
    data.loc[(data["전입자10_만나이"] >= 70) & (data["전입자10_만나이"] < 75),"전입자10_나이"] = "70 - 74세"
    data.loc[(data["전입자10_만나이"] >= 75) & (data["전입자10_만나이"] < 80),"전입자10_나이"] = "75 - 79세"
    data.loc[(data["전입자10_만나이"] >= 80) & (data["전입자10_만나이"] < 85),"전입자10_나이"] = "80 - 84세"
    data.loc[(data["전입자10_만나이"] >= 85) & (data["전입자10_만나이"] < 90),"전입자10_나이"] = "85 - 89세"
    data.loc[(data["전입자10_만나이"] >= 90),"전입자10_나이"] = "90세 이상"

    data["전입자10_나이"] = pd.Categorical(data["전입자10_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    tmp = data[["전입행정_시도","전입행정_시군구", "전입행정_동읍면", "전입사유",
                  "전입자1_성별", "전입자1_나이", "전입자2_성별", "전입자2_나이",
                  "전입자3_성별", "전입자3_나이", "전입자4_성별", "전입자4_나이",
                  "전입자5_성별", "전입자5_나이", "전입자6_성별", "전입자6_나이",
                  "전입자7_성별", "전입자7_나이", "전입자8_성별", "전입자8_나이",
                  "전입자9_성별", "전입자9_나이", "전입자10_성별", "전입자10_나이", "전입년"]]

    tmp = pd.melt(tmp,["전입행정_시도","전입행정_시군구", "전입행정_동읍면", "전입사유",
                      "전입자1_성별", "전입자2_성별", "전입자3_성별", "전입자4_성별",
                       "전입자5_성별", "전입자6_성별", "전입자7_성별", "전입자8_성별",
                      "전입자9_성별", "전입자10_성별", "전입년"],
                      ["전입자1_나이", "전입자2_나이", "전입자3_나이", "전입자4_나이"
                                , "전입자5_나이", "전입자6_나이", "전입자7_나이", "전입자8_나이"
                                , "전입자9_나이", "전입자10_나이"],"전입자","나이")
    tmp1 = tmp.dropna(subset=['나이'])

    for factor in tmp1["전입자"].drop_duplicates() :
        tmp1.loc[tmp1["전입자"]== factor, "성별"] = tmp1[factor[0:-3]+"_성별"]

    tmp1["성별"] = pd.Categorical(tmp1["성별"],categories=["남자","여자"],ordered=False)
    tmp2 = tmp1[["전입행정_시도","전입행정_시군구","전입행정_동읍면","전입사유","전입년","나이","성별"]]
    tmp2["전입년"] = "전입_" + tmp2["전입년"].map(str)
    tmp2.to_csv("/home/infra/user/ya/param/in_tmp2.csv", mode ="w")
    tmp2 = pd.read_csv('/home/infra/user/ya/param/in_tmp2.csv')
    tmp3 = pd.DataFrame({'전입':tmp2.groupby(["전입행정_시군구", "전입행정_동읍면", "전입년", "성별", "나이"]).size()}).reset_index()
    tmp3.sort_values(by=["전입행정_시군구","전입행정_동읍면"],axis=0,ascending=False)
    tmp3.to_csv('/home/infra/user/ya/param/in_tmp3.csv',mode="w",index=False)
    tmp3 = pd.read_csv('/home/infra/user/ya/param/in_tmp3.csv')
    tmp4 = tmp3.pivot_table(index=["전입행정_시군구", "전입행정_동읍면", "성별", "나이"], columns='전입년', values='전입').reset_index()
    tmp4 = tmp4.fillna(0)
    return tmp4


def moveout_preprocess(data):
    data.columns = ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10",
                  "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20",
                  "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "V29", "V30",
                  "V31", "V32", "V33", "V34", "V35", "V36", "V37", "V38", "V39", "V40", "V41"]

    data["V1"] = data["V1"].astype("str")
    data["V7"] = data["V7"].astype("str")
    data["V2"] = data["V1"].map(str) + data["V2"].map(str)
    data["V3"] = data["V2"].map(str) + data["V3"].map(str)
    data["V8"] = data["V7"].map(str) + data["V8"].map(str)
    data["V9"] = data["V8"].map(str) + data["V9"].map(str)

    data = pd.merge(data, mapping, left_on = "V1",right_on="a",how='left')
    data.loc[:,"V1"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V2",right_on="a",how='left')
    data.loc[:,"V2"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V3",right_on="a",how='left')
    data.loc[:,"V3"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V7",right_on="a",how='left')
    data.loc[:,"V7"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V8",right_on="a",how='left')
    data.loc[:,"V8"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)
    data = pd.merge(data, mapping, left_on = "V9",right_on="a",how='left')
    data.loc[:,"V9"] = data.loc[:,"b"]
    data.drop(["a","b"],axis=1,inplace=True)

    data.loc[data["V13"]==0,"V13"] = "여자"
    data.loc[data["V13"]==1,"V13"] = "남자"
    data.loc[data["V13"]==2,"V13"] = "여자"
    data.loc[data["V13"]==3,"V13"] = "남자"
    data.loc[data["V13"]==4,"V13"] = "여자"
    data.loc[data["V13"]==9,"V13"] = "남자"
    data.loc[data["V16"]==0,"V25"] = "여자"
    data.loc[data["V16"]==1,"V25"] = "남자"
    data.loc[data["V16"]==2,"V16"] = "여자"
    data.loc[data["V16"]==3,"V16"] = "남자"
    data.loc[data["V16"]==4,"V16"] = "여자"
    data.loc[data["V16"]==9,"V16"] = "남자"
    data.loc[data["V19"]==0,"V19"] = "여자"
    data.loc[data["V19"]==1,"V19"] = "남자"
    data.loc[data["V19"]==2,"V19"] = "여자"
    data.loc[data["V19"]==3,"V19"] = "남자"
    data.loc[data["V19"]==4,"V19"] = "여자"
    data.loc[data["V19"]==9,"V19"] = "남자"
    data.loc[data["V22"]==0,"V22"] = "여자"
    data.loc[data["V22"]==1,"V22"] = "남자"
    data.loc[data["V22"]==2,"V22"] = "여자"
    data.loc[data["V22"]==3,"V22"] = "남자"
    data.loc[data["V22"]==4,"V22"] = "여자"
    data.loc[data["V22"]==9,"V22"] = "남자"
    data.loc[data["V25"]==0,"V25"] = "여자"
    data.loc[data["V25"]==1,"V25"] = "남자"
    data.loc[data["V25"]==2,"V25"] = "여자"
    data.loc[data["V25"]==3,"V25"] = "남자"
    data.loc[data["V25"]==4,"V25"] = "여자"
    data.loc[data["V25"]==9,"V25"] = "남자"
    data.loc[data["V28"]==0,"V28"] = "여자"
    data.loc[data["V28"]==1,"V28"] = "남자"
    data.loc[data["V28"]==2,"V28"] = "여자"
    data.loc[data["V28"]==3,"V28"] = "남자"
    data.loc[data["V28"]==4,"V28"] = "여자"
    data.loc[data["V28"]==9,"V28"] = "남자"
    data.loc[data["V31"]==0,"V31"] = "여자"
    data.loc[data["V31"]==1,"V31"] = "남자"
    data.loc[data["V31"]==2,"V31"] = "여자"
    data.loc[data["V31"]==3,"V31"] = "남자"
    data.loc[data["V31"]==4,"V31"] = "여자"
    data.loc[data["V31"]==9,"V31"] = "남자"
    data.loc[data["V34"]==0,"V34"] = "여자"
    data.loc[data["V34"]==1,"V34"] = "남자"
    data.loc[data["V34"]==2,"V34"] = "여자"
    data.loc[data["V34"]==3,"V34"] = "남자"
    data.loc[data["V34"]==4,"V34"] = "여자"
    data.loc[data["V34"]==9,"V34"] = "남자"
    data.loc[data["V37"]==0,"V37"] = "여자"
    data.loc[data["V37"]==1,"V37"] = "남자"
    data.loc[data["V37"]==2,"V37"] = "여자"
    data.loc[data["V37"]==3,"V37"] = "남자"
    data.loc[data["V37"]==4,"V37"] = "여자"
    data.loc[data["V37"]==9,"V37"] = "남자"
    data.loc[data["V40"]==0,"V40"] = "여자"
    data.loc[data["V40"]==1,"V40"] = "남자"
    data.loc[data["V40"]==2,"V40"] = "여자"
    data.loc[data["V40"]==3,"V40"] = "남자"
    data.loc[data["V40"]==4,"V40"] = "여자"
    data.loc[data["V40"]==9,"V40"] = "남자"

    data.columns = ["전입행정_시도", "전입행정_시군구", "전입행정_동읍면", "전입년", "전입월", "전입일",
                  "전출행정_시도", "전출행정_시군구", "전출행정_동읍면", "전입사유", "전입자1_관계",
                  "전입자1_만나이", "전입자1_성별", "전입자2_관계", "전입자2_만나이", "전입자2_성별",
                  "전입자3_관계", "전입자3_만나이", "전입자3_성별", "전입자4_관계", "전입자4_만나이",
                  "전입자4_성별", "전입자5_관계", "전입자5_만나이", "전입자5_성별", "전입자6_관계",
                  "전입자6_만나이", "전입자6_성별", "전입자7_관계", "전입자7_만나이", "전입자7_성별",
                  "전입자8_관계", "전입자8_만나이", "전입자8_성별", "전입자9_관계", "전입자9_만나이",
                  "전입자9_성별", "전입자10_관계", "전입자10_만나이", "전입자10_성별", "일련번호"]

    data.to_csv('/home/infra/user/ya/param/moveout.csv',mode="w")

def age_factor_out(data) :
    data.loc[data["전입자1_만나이"] <5,"전입자1_나이"] = "0-4세"
    data.loc[(data["전입자1_만나이"] >= 5) & (data["전입자1_만나이"] < 10),"전입자1_나이"] = "5 - 9세"
    data.loc[(data["전입자1_만나이"] >= 10) & (data["전입자1_만나이"] < 15),"전입자1_나이"] = "10 - 14세"
    data.loc[(data["전입자1_만나이"] >= 15) & (data["전입자1_만나이"] < 20),"전입자1_나이"] = "15 - 19세"
    data.loc[(data["전입자1_만나이"] >= 20) & (data["전입자1_만나이"] < 25),"전입자1_나이"] = "20 - 24세"
    data.loc[(data["전입자1_만나이"] >= 25) & (data["전입자1_만나이"] < 30),"전입자1_나이"] = "25 - 29세"
    data.loc[(data["전입자1_만나이"] >= 30) & (data["전입자1_만나이"] < 35),"전입자1_나이"] = "30 - 34세"
    data.loc[(data["전입자1_만나이"] >= 35) & (data["전입자1_만나이"] < 40),"전입자1_나이"] = "35 - 39세"
    data.loc[(data["전입자1_만나이"] >= 40) & (data["전입자1_만나이"] < 45),"전입자1_나이"] = "40 - 44세"
    data.loc[(data["전입자1_만나이"] >= 45) & (data["전입자1_만나이"] < 50),"전입자1_나이"] = "45 - 49세"
    data.loc[(data["전입자1_만나이"] >= 50) & (data["전입자1_만나이"] < 55),"전입자1_나이"] = "50 - 54세"
    data.loc[(data["전입자1_만나이"] >= 55) & (data["전입자1_만나이"] < 60),"전입자1_나이"] = "55 - 59세"
    data.loc[(data["전입자1_만나이"] >= 60) & (data["전입자1_만나이"] < 65),"전입자1_나이"] = "60 - 64세"
    data.loc[(data["전입자1_만나이"] >= 65) & (data["전입자1_만나이"] < 70),"전입자1_나이"] = "65 - 69세"
    data.loc[(data["전입자1_만나이"] >= 70) & (data["전입자1_만나이"] < 75),"전입자1_나이"] = "70 - 74세"
    data.loc[(data["전입자1_만나이"] >= 75) & (data["전입자1_만나이"] < 80),"전입자1_나이"] = "75 - 79세"
    data.loc[(data["전입자1_만나이"] >= 80) & (data["전입자1_만나이"] < 85),"전입자1_나이"] = "80 - 84세"
    data.loc[(data["전입자1_만나이"] >= 85) & (data["전입자1_만나이"] < 90),"전입자1_나이"] = "85 - 89세"
    data.loc[(data["전입자1_만나이"] >= 90),"전입자1_나이"] = "90세 이상"

    data["전입자1_나이"] = pd.Categorical(data["전입자1_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자2_만나이"] <5,"전입자2_나이"] = "0-4세"
    data.loc[(data["전입자2_만나이"] >= 5) & (data["전입자2_만나이"] < 10),"전입자2_나이"] = "5 - 9세"
    data.loc[(data["전입자2_만나이"] >= 10) & (data["전입자2_만나이"] < 15),"전입자2_나이"] = "10 - 14세"
    data.loc[(data["전입자2_만나이"] >= 15) & (data["전입자2_만나이"] < 20),"전입자2_나이"] = "15 - 19세"
    data.loc[(data["전입자2_만나이"] >= 20) & (data["전입자2_만나이"] < 25),"전입자2_나이"] = "20 - 24세"
    data.loc[(data["전입자2_만나이"] >= 25) & (data["전입자2_만나이"] < 30),"전입자2_나이"] = "25 - 29세"
    data.loc[(data["전입자2_만나이"] >= 30) & (data["전입자2_만나이"] < 35),"전입자2_나이"] = "30 - 34세"
    data.loc[(data["전입자2_만나이"] >= 35) & (data["전입자2_만나이"] < 40),"전입자2_나이"] = "35 - 39세"
    data.loc[(data["전입자2_만나이"] >= 40) & (data["전입자2_만나이"] < 45),"전입자2_나이"] = "40 - 44세"
    data.loc[(data["전입자2_만나이"] >= 45) & (data["전입자2_만나이"] < 50),"전입자2_나이"] = "45 - 49세"
    data.loc[(data["전입자2_만나이"] >= 50) & (data["전입자2_만나이"] < 55),"전입자2_나이"] = "50 - 54세"
    data.loc[(data["전입자2_만나이"] >= 55) & (data["전입자2_만나이"] < 60),"전입자2_나이"] = "55 - 59세"
    data.loc[(data["전입자2_만나이"] >= 60) & (data["전입자2_만나이"] < 65),"전입자2_나이"] = "60 - 64세"
    data.loc[(data["전입자2_만나이"] >= 65) & (data["전입자2_만나이"] < 70),"전입자2_나이"] = "65 - 69세"
    data.loc[(data["전입자2_만나이"] >= 70) & (data["전입자2_만나이"] < 75),"전입자2_나이"] = "70 - 74세"
    data.loc[(data["전입자2_만나이"] >= 75) & (data["전입자2_만나이"] < 80),"전입자2_나이"] = "75 - 79세"
    data.loc[(data["전입자2_만나이"] >= 80) & (data["전입자2_만나이"] < 85),"전입자2_나이"] = "80 - 84세"
    data.loc[(data["전입자2_만나이"] >= 85) & (data["전입자2_만나이"] < 90),"전입자2_나이"] = "85 - 89세"
    data.loc[(data["전입자2_만나이"] >= 90),"전입자2_나이"] = "90세 이상"

    data["전입자2_나이"] = pd.Categorical(data["전입자2_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자3_만나이"] <5,"전입자3_나이"] = "0-4세"
    data.loc[(data["전입자3_만나이"] >= 5) & (data["전입자3_만나이"] < 10),"전입자3_나이"] = "5 - 9세"
    data.loc[(data["전입자3_만나이"] >= 10) & (data["전입자3_만나이"] < 15),"전입자3_나이"] = "10 - 14세"
    data.loc[(data["전입자3_만나이"] >= 15) & (data["전입자3_만나이"] < 20),"전입자3_나이"] = "15 - 19세"
    data.loc[(data["전입자3_만나이"] >= 20) & (data["전입자3_만나이"] < 25),"전입자3_나이"] = "20 - 24세"
    data.loc[(data["전입자3_만나이"] >= 25) & (data["전입자3_만나이"] < 30),"전입자3_나이"] = "25 - 29세"
    data.loc[(data["전입자3_만나이"] >= 30) & (data["전입자3_만나이"] < 35),"전입자3_나이"] = "30 - 34세"
    data.loc[(data["전입자3_만나이"] >= 35) & (data["전입자3_만나이"] < 40),"전입자3_나이"] = "35 - 39세"
    data.loc[(data["전입자3_만나이"] >= 40) & (data["전입자3_만나이"] < 45),"전입자3_나이"] = "40 - 44세"
    data.loc[(data["전입자3_만나이"] >= 45) & (data["전입자3_만나이"] < 50),"전입자3_나이"] = "45 - 49세"
    data.loc[(data["전입자3_만나이"] >= 50) & (data["전입자3_만나이"] < 55),"전입자3_나이"] = "50 - 54세"
    data.loc[(data["전입자3_만나이"] >= 55) & (data["전입자3_만나이"] < 60),"전입자3_나이"] = "55 - 59세"
    data.loc[(data["전입자3_만나이"] >= 60) & (data["전입자3_만나이"] < 65),"전입자3_나이"] = "60 - 64세"
    data.loc[(data["전입자3_만나이"] >= 65) & (data["전입자3_만나이"] < 70),"전입자3_나이"] = "65 - 69세"
    data.loc[(data["전입자3_만나이"] >= 70) & (data["전입자3_만나이"] < 75),"전입자3_나이"] = "70 - 74세"
    data.loc[(data["전입자3_만나이"] >= 75) & (data["전입자3_만나이"] < 80),"전입자3_나이"] = "75 - 79세"
    data.loc[(data["전입자3_만나이"] >= 80) & (data["전입자3_만나이"] < 85),"전입자3_나이"] = "80 - 84세"
    data.loc[(data["전입자3_만나이"] >= 85) & (data["전입자3_만나이"] < 90),"전입자3_나이"] = "85 - 89세"
    data.loc[(data["전입자3_만나이"] >= 90),"전입자3_나이"] = "90세 이상"

    data["전입자3_나이"] = pd.Categorical(data["전입자3_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자4_만나이"] <5,"전입자4_나이"] = "0-4세"
    data.loc[(data["전입자4_만나이"] >= 5) & (data["전입자4_만나이"] < 10),"전입자4_나이"] = "5 - 9세"
    data.loc[(data["전입자4_만나이"] >= 10) & (data["전입자4_만나이"] < 15),"전입자4_나이"] = "10 - 14세"
    data.loc[(data["전입자4_만나이"] >= 15) & (data["전입자4_만나이"] < 20),"전입자4_나이"] = "15 - 19세"
    data.loc[(data["전입자4_만나이"] >= 20) & (data["전입자4_만나이"] < 25),"전입자4_나이"] = "20 - 24세"
    data.loc[(data["전입자4_만나이"] >= 25) & (data["전입자4_만나이"] < 30),"전입자4_나이"] = "25 - 29세"
    data.loc[(data["전입자4_만나이"] >= 30) & (data["전입자4_만나이"] < 35),"전입자4_나이"] = "30 - 34세"
    data.loc[(data["전입자4_만나이"] >= 35) & (data["전입자4_만나이"] < 40),"전입자4_나이"] = "35 - 39세"
    data.loc[(data["전입자4_만나이"] >= 40) & (data["전입자4_만나이"] < 45),"전입자4_나이"] = "40 - 44세"
    data.loc[(data["전입자4_만나이"] >= 45) & (data["전입자4_만나이"] < 50),"전입자4_나이"] = "45 - 49세"
    data.loc[(data["전입자4_만나이"] >= 50) & (data["전입자4_만나이"] < 55),"전입자4_나이"] = "50 - 54세"
    data.loc[(data["전입자4_만나이"] >= 55) & (data["전입자4_만나이"] < 60),"전입자4_나이"] = "55 - 59세"
    data.loc[(data["전입자4_만나이"] >= 60) & (data["전입자4_만나이"] < 65),"전입자4_나이"] = "60 - 64세"
    data.loc[(data["전입자4_만나이"] >= 65) & (data["전입자4_만나이"] < 70),"전입자4_나이"] = "65 - 69세"
    data.loc[(data["전입자4_만나이"] >= 70) & (data["전입자4_만나이"] < 75),"전입자4_나이"] = "70 - 74세"
    data.loc[(data["전입자4_만나이"] >= 75) & (data["전입자4_만나이"] < 80),"전입자4_나이"] = "75 - 79세"
    data.loc[(data["전입자4_만나이"] >= 80) & (data["전입자4_만나이"] < 85),"전입자4_나이"] = "80 - 84세"
    data.loc[(data["전입자4_만나이"] >= 85) & (data["전입자4_만나이"] < 90),"전입자4_나이"] = "85 - 89세"
    data.loc[(data["전입자4_만나이"] >= 90),"전입자4_나이"] = "90세 이상"

    data["전입자4_나이"] = pd.Categorical(data["전입자4_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자5_만나이"] <5,"전입자5_나이"] = "0-4세"
    data.loc[(data["전입자5_만나이"] >= 5) & (data["전입자5_만나이"] < 10),"전입자5_나이"] = "5 - 9세"
    data.loc[(data["전입자5_만나이"] >= 10) & (data["전입자5_만나이"] < 15),"전입자5_나이"] = "10 - 14세"
    data.loc[(data["전입자5_만나이"] >= 15) & (data["전입자5_만나이"] < 20),"전입자5_나이"] = "15 - 19세"
    data.loc[(data["전입자5_만나이"] >= 20) & (data["전입자5_만나이"] < 25),"전입자5_나이"] = "20 - 24세"
    data.loc[(data["전입자5_만나이"] >= 25) & (data["전입자5_만나이"] < 30),"전입자5_나이"] = "25 - 29세"
    data.loc[(data["전입자5_만나이"] >= 30) & (data["전입자5_만나이"] < 35),"전입자5_나이"] = "30 - 34세"
    data.loc[(data["전입자5_만나이"] >= 35) & (data["전입자5_만나이"] < 40),"전입자5_나이"] = "35 - 39세"
    data.loc[(data["전입자5_만나이"] >= 40) & (data["전입자5_만나이"] < 45),"전입자5_나이"] = "40 - 44세"
    data.loc[(data["전입자5_만나이"] >= 45) & (data["전입자5_만나이"] < 50),"전입자5_나이"] = "45 - 49세"
    data.loc[(data["전입자5_만나이"] >= 50) & (data["전입자5_만나이"] < 55),"전입자5_나이"] = "50 - 54세"
    data.loc[(data["전입자5_만나이"] >= 55) & (data["전입자5_만나이"] < 60),"전입자5_나이"] = "55 - 59세"
    data.loc[(data["전입자5_만나이"] >= 60) & (data["전입자5_만나이"] < 65),"전입자5_나이"] = "60 - 64세"
    data.loc[(data["전입자5_만나이"] >= 65) & (data["전입자5_만나이"] < 70),"전입자5_나이"] = "65 - 69세"
    data.loc[(data["전입자5_만나이"] >= 70) & (data["전입자5_만나이"] < 75),"전입자5_나이"] = "70 - 74세"
    data.loc[(data["전입자5_만나이"] >= 75) & (data["전입자5_만나이"] < 80),"전입자5_나이"] = "75 - 79세"
    data.loc[(data["전입자5_만나이"] >= 80) & (data["전입자5_만나이"] < 85),"전입자5_나이"] = "80 - 84세"
    data.loc[(data["전입자5_만나이"] >= 85) & (data["전입자5_만나이"] < 90),"전입자5_나이"] = "85 - 89세"
    data.loc[(data["전입자5_만나이"] >= 90),"전입자5_나이"] = "90세 이상"

    data["전입자5_나이"] = pd.Categorical(data["전입자5_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자6_만나이"] <5,"전입자6_나이"] = "0-4세"
    data.loc[(data["전입자6_만나이"] >= 5) & (data["전입자6_만나이"] < 10),"전입자6_나이"] = "5 - 9세"
    data.loc[(data["전입자6_만나이"] >= 10) & (data["전입자6_만나이"] < 15),"전입자6_나이"] = "10 - 14세"
    data.loc[(data["전입자6_만나이"] >= 15) & (data["전입자6_만나이"] < 20),"전입자6_나이"] = "15 - 19세"
    data.loc[(data["전입자6_만나이"] >= 20) & (data["전입자6_만나이"] < 25),"전입자6_나이"] = "20 - 24세"
    data.loc[(data["전입자6_만나이"] >= 25) & (data["전입자6_만나이"] < 30),"전입자6_나이"] = "25 - 29세"
    data.loc[(data["전입자6_만나이"] >= 30) & (data["전입자6_만나이"] < 35),"전입자6_나이"] = "30 - 34세"
    data.loc[(data["전입자6_만나이"] >= 35) & (data["전입자6_만나이"] < 40),"전입자6_나이"] = "35 - 39세"
    data.loc[(data["전입자6_만나이"] >= 40) & (data["전입자6_만나이"] < 45),"전입자6_나이"] = "40 - 44세"
    data.loc[(data["전입자6_만나이"] >= 45) & (data["전입자6_만나이"] < 50),"전입자6_나이"] = "45 - 49세"
    data.loc[(data["전입자6_만나이"] >= 50) & (data["전입자6_만나이"] < 55),"전입자6_나이"] = "50 - 54세"
    data.loc[(data["전입자6_만나이"] >= 55) & (data["전입자6_만나이"] < 60),"전입자6_나이"] = "55 - 59세"
    data.loc[(data["전입자6_만나이"] >= 60) & (data["전입자6_만나이"] < 65),"전입자6_나이"] = "60 - 64세"
    data.loc[(data["전입자6_만나이"] >= 65) & (data["전입자6_만나이"] < 70),"전입자6_나이"] = "65 - 69세"
    data.loc[(data["전입자6_만나이"] >= 70) & (data["전입자6_만나이"] < 75),"전입자6_나이"] = "70 - 74세"
    data.loc[(data["전입자6_만나이"] >= 75) & (data["전입자6_만나이"] < 80),"전입자6_나이"] = "75 - 79세"
    data.loc[(data["전입자6_만나이"] >= 80) & (data["전입자6_만나이"] < 85),"전입자6_나이"] = "80 - 84세"
    data.loc[(data["전입자6_만나이"] >= 85) & (data["전입자6_만나이"] < 90),"전입자6_나이"] = "85 - 89세"
    data.loc[(data["전입자6_만나이"] >= 90),"전입자6_나이"] = "90세 이상"

    data["전입자6_나이"] = pd.Categorical(data["전입자6_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자7_만나이"] <5,"전입자7_나이"] = "0-4세"
    data.loc[(data["전입자7_만나이"] >= 5) & (data["전입자7_만나이"] < 10),"전입자7_나이"] = "5 - 9세"
    data.loc[(data["전입자7_만나이"] >= 10) & (data["전입자7_만나이"] < 15),"전입자7_나이"] = "10 - 14세"
    data.loc[(data["전입자7_만나이"] >= 15) & (data["전입자7_만나이"] < 20),"전입자7_나이"] = "15 - 19세"
    data.loc[(data["전입자7_만나이"] >= 20) & (data["전입자7_만나이"] < 25),"전입자7_나이"] = "20 - 24세"
    data.loc[(data["전입자7_만나이"] >= 25) & (data["전입자7_만나이"] < 30),"전입자7_나이"] = "25 - 29세"
    data.loc[(data["전입자7_만나이"] >= 30) & (data["전입자7_만나이"] < 35),"전입자7_나이"] = "30 - 34세"
    data.loc[(data["전입자7_만나이"] >= 35) & (data["전입자7_만나이"] < 40),"전입자7_나이"] = "35 - 39세"
    data.loc[(data["전입자7_만나이"] >= 40) & (data["전입자7_만나이"] < 45),"전입자7_나이"] = "40 - 44세"
    data.loc[(data["전입자7_만나이"] >= 45) & (data["전입자7_만나이"] < 50),"전입자7_나이"] = "45 - 49세"
    data.loc[(data["전입자7_만나이"] >= 50) & (data["전입자7_만나이"] < 55),"전입자7_나이"] = "50 - 54세"
    data.loc[(data["전입자7_만나이"] >= 55) & (data["전입자7_만나이"] < 60),"전입자7_나이"] = "55 - 59세"
    data.loc[(data["전입자7_만나이"] >= 60) & (data["전입자7_만나이"] < 65),"전입자7_나이"] = "60 - 64세"
    data.loc[(data["전입자7_만나이"] >= 65) & (data["전입자7_만나이"] < 70),"전입자7_나이"] = "65 - 69세"
    data.loc[(data["전입자7_만나이"] >= 70) & (data["전입자7_만나이"] < 75),"전입자7_나이"] = "70 - 74세"
    data.loc[(data["전입자7_만나이"] >= 75) & (data["전입자7_만나이"] < 80),"전입자7_나이"] = "75 - 79세"
    data.loc[(data["전입자7_만나이"] >= 80) & (data["전입자7_만나이"] < 85),"전입자7_나이"] = "80 - 84세"
    data.loc[(data["전입자7_만나이"] >= 85) & (data["전입자7_만나이"] < 90),"전입자7_나이"] = "85 - 89세"
    data.loc[(data["전입자7_만나이"] >= 90),"전입자7_나이"] = "90세 이상"

    data["전입자7_나이"] = pd.Categorical(data["전입자7_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자8_만나이"] <5,"전입자8_나이"] = "0-4세"
    data.loc[(data["전입자8_만나이"] >= 5) & (data["전입자8_만나이"] < 10),"전입자8_나이"] = "5 - 9세"
    data.loc[(data["전입자8_만나이"] >= 10) & (data["전입자8_만나이"] < 15),"전입자8_나이"] = "10 - 14세"
    data.loc[(data["전입자8_만나이"] >= 15) & (data["전입자8_만나이"] < 20),"전입자8_나이"] = "15 - 19세"
    data.loc[(data["전입자8_만나이"] >= 20) & (data["전입자8_만나이"] < 25),"전입자8_나이"] = "20 - 24세"
    data.loc[(data["전입자8_만나이"] >= 25) & (data["전입자8_만나이"] < 30),"전입자8_나이"] = "25 - 29세"
    data.loc[(data["전입자8_만나이"] >= 30) & (data["전입자8_만나이"] < 35),"전입자8_나이"] = "30 - 34세"
    data.loc[(data["전입자8_만나이"] >= 35) & (data["전입자8_만나이"] < 40),"전입자8_나이"] = "35 - 39세"
    data.loc[(data["전입자8_만나이"] >= 40) & (data["전입자8_만나이"] < 45),"전입자8_나이"] = "40 - 44세"
    data.loc[(data["전입자8_만나이"] >= 45) & (data["전입자8_만나이"] < 50),"전입자8_나이"] = "45 - 49세"
    data.loc[(data["전입자8_만나이"] >= 50) & (data["전입자8_만나이"] < 55),"전입자8_나이"] = "50 - 54세"
    data.loc[(data["전입자8_만나이"] >= 55) & (data["전입자8_만나이"] < 60),"전입자8_나이"] = "55 - 59세"
    data.loc[(data["전입자8_만나이"] >= 60) & (data["전입자8_만나이"] < 65),"전입자8_나이"] = "60 - 64세"
    data.loc[(data["전입자8_만나이"] >= 65) & (data["전입자8_만나이"] < 70),"전입자8_나이"] = "65 - 69세"
    data.loc[(data["전입자8_만나이"] >= 70) & (data["전입자8_만나이"] < 75),"전입자8_나이"] = "70 - 74세"
    data.loc[(data["전입자8_만나이"] >= 75) & (data["전입자8_만나이"] < 80),"전입자8_나이"] = "75 - 79세"
    data.loc[(data["전입자8_만나이"] >= 80) & (data["전입자8_만나이"] < 85),"전입자8_나이"] = "80 - 84세"
    data.loc[(data["전입자8_만나이"] >= 85) & (data["전입자8_만나이"] < 90),"전입자8_나이"] = "85 - 89세"
    data.loc[(data["전입자8_만나이"] >= 90),"전입자8_나이"] = "90세 이상"

    data["전입자8_나이"] = pd.Categorical(data["전입자8_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)

    data.loc[data["전입자9_만나이"] <5,"전입자9_나이"] = "0-4세"
    data.loc[(data["전입자9_만나이"] >= 5) & (data["전입자9_만나이"] < 10),"전입자9_나이"] = "5 - 9세"
    data.loc[(data["전입자9_만나이"] >= 10) & (data["전입자9_만나이"] < 15),"전입자9_나이"] = "10 - 14세"
    data.loc[(data["전입자9_만나이"] >= 15) & (data["전입자9_만나이"] < 20),"전입자9_나이"] = "15 - 19세"
    data.loc[(data["전입자9_만나이"] >= 20) & (data["전입자9_만나이"] < 25),"전입자9_나이"] = "20 - 24세"
    data.loc[(data["전입자9_만나이"] >= 25) & (data["전입자9_만나이"] < 30),"전입자9_나이"] = "25 - 29세"
    data.loc[(data["전입자9_만나이"] >= 30) & (data["전입자9_만나이"] < 35),"전입자9_나이"] = "30 - 34세"
    data.loc[(data["전입자9_만나이"] >= 35) & (data["전입자9_만나이"] < 40),"전입자9_나이"] = "35 - 39세"
    data.loc[(data["전입자9_만나이"] >= 40) & (data["전입자9_만나이"] < 45),"전입자9_나이"] = "40 - 44세"
    data.loc[(data["전입자9_만나이"] >= 45) & (data["전입자9_만나이"] < 50),"전입자9_나이"] = "45 - 49세"
    data.loc[(data["전입자9_만나이"] >= 50) & (data["전입자9_만나이"] < 55),"전입자9_나이"] = "50 - 54세"
    data.loc[(data["전입자9_만나이"] >= 55) & (data["전입자9_만나이"] < 60),"전입자9_나이"] = "55 - 59세"
    data.loc[(data["전입자9_만나이"] >= 60) & (data["전입자9_만나이"] < 65),"전입자9_나이"] = "60 - 64세"
    data.loc[(data["전입자9_만나이"] >= 65) & (data["전입자9_만나이"] < 70),"전입자9_나이"] = "65 - 69세"
    data.loc[(data["전입자9_만나이"] >= 70) & (data["전입자9_만나이"] < 75),"전입자9_나이"] = "70 - 74세"
    data.loc[(data["전입자9_만나이"] >= 75) & (data["전입자9_만나이"] < 80),"전입자9_나이"] = "75 - 79세"
    data.loc[(data["전입자9_만나이"] >= 80) & (data["전입자9_만나이"] < 85),"전입자9_나이"] = "80 - 84세"
    data.loc[(data["전입자9_만나이"] >= 85) & (data["전입자9_만나이"] < 90),"전입자9_나이"] = "85 - 89세"
    data.loc[(data["전입자9_만나이"] >= 90),"전입자9_나이"] = "90세 이상"

    data["전입자9_나이"] = pd.Categorical(data["전입자9_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    data.loc[data["전입자10_만나이"] <5,"전입자10_나이"] = "0-4세"
    data.loc[(data["전입자10_만나이"] >= 5) & (data["전입자10_만나이"] < 10),"전입자10_나이"] = "5 - 9세"
    data.loc[(data["전입자10_만나이"] >= 10) & (data["전입자10_만나이"] < 15),"전입자10_나이"] = "10 - 14세"
    data.loc[(data["전입자10_만나이"] >= 15) & (data["전입자10_만나이"] < 20),"전입자10_나이"] = "15 - 19세"
    data.loc[(data["전입자10_만나이"] >= 20) & (data["전입자10_만나이"] < 25),"전입자10_나이"] = "20 - 24세"
    data.loc[(data["전입자10_만나이"] >= 25) & (data["전입자10_만나이"] < 30),"전입자10_나이"] = "25 - 29세"
    data.loc[(data["전입자10_만나이"] >= 30) & (data["전입자10_만나이"] < 35),"전입자10_나이"] = "30 - 34세"
    data.loc[(data["전입자10_만나이"] >= 35) & (data["전입자10_만나이"] < 40),"전입자10_나이"] = "35 - 39세"
    data.loc[(data["전입자10_만나이"] >= 40) & (data["전입자10_만나이"] < 45),"전입자10_나이"] = "40 - 44세"
    data.loc[(data["전입자10_만나이"] >= 45) & (data["전입자10_만나이"] < 50),"전입자10_나이"] = "45 - 49세"
    data.loc[(data["전입자10_만나이"] >= 50) & (data["전입자10_만나이"] < 55),"전입자10_나이"] = "50 - 54세"
    data.loc[(data["전입자10_만나이"] >= 55) & (data["전입자10_만나이"] < 60),"전입자10_나이"] = "55 - 59세"
    data.loc[(data["전입자10_만나이"] >= 60) & (data["전입자10_만나이"] < 65),"전입자10_나이"] = "60 - 64세"
    data.loc[(data["전입자10_만나이"] >= 65) & (data["전입자10_만나이"] < 70),"전입자10_나이"] = "65 - 69세"
    data.loc[(data["전입자10_만나이"] >= 70) & (data["전입자10_만나이"] < 75),"전입자10_나이"] = "70 - 74세"
    data.loc[(data["전입자10_만나이"] >= 75) & (data["전입자10_만나이"] < 80),"전입자10_나이"] = "75 - 79세"
    data.loc[(data["전입자10_만나이"] >= 80) & (data["전입자10_만나이"] < 85),"전입자10_나이"] = "80 - 84세"
    data.loc[(data["전입자10_만나이"] >= 85) & (data["전입자10_만나이"] < 90),"전입자10_나이"] = "85 - 89세"
    data.loc[(data["전입자10_만나이"] >= 90),"전입자10_나이"] = "90세 이상"

    data["전입자10_나이"] = pd.Categorical(data["전입자10_나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                    "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                    "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                    "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                    "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)


    tmp = data[["전출행정_시도","전출행정_시군구", "전출행정_동읍면", "전입사유",
                  "전입자1_성별", "전입자1_나이", "전입자2_성별", "전입자2_나이",
                  "전입자3_성별", "전입자3_나이", "전입자4_성별", "전입자4_나이",
                  "전입자5_성별", "전입자5_나이", "전입자6_성별", "전입자6_나이",
                  "전입자7_성별", "전입자7_나이", "전입자8_성별", "전입자8_나이",
                  "전입자9_성별", "전입자9_나이", "전입자10_성별", "전입자10_나이", "전입년"]]

    tmp = pd.melt(tmp,["전출행정_시도","전출행정_시군구", "전출행정_동읍면", "전입사유",
                      "전입자1_성별", "전입자2_성별", "전입자3_성별", "전입자4_성별",
                       "전입자5_성별", "전입자6_성별", "전입자7_성별", "전입자8_성별",
                      "전입자9_성별", "전입자10_성별", "전입년"],
                      ["전입자1_나이", "전입자2_나이", "전입자3_나이", "전입자4_나이"
                                , "전입자5_나이", "전입자6_나이", "전입자7_나이", "전입자8_나이"
                                , "전입자9_나이", "전입자10_나이"],"전출자","나이")
    tmp1 = tmp.dropna(subset=['나이'])

    for factor in tmp1["전출자"].drop_duplicates() :
        tmp1.loc[tmp1["전출자"]== factor, "성별"] = tmp1[factor[0:-3]+"_성별"]

    tmp1["성별"] = pd.Categorical(tmp1["성별"],categories=["남자","여자"],ordered=False)
    tmp2 = tmp1[["전출행정_시도","전출행정_시군구","전출행정_동읍면","전입사유","전입년","나이","성별"]]
    tmp2["전입년"] = "전출_" + tmp2["전입년"].map(str)
    tmp2.to_csv("/home/infra/user/ya/out_tmp2.csv", mode ="w")
    tmp2 = pd.read_csv('/home/infra/user/ya/out_tmp2.csv')
    tmp3 = pd.DataFrame({'전출':tmp2.groupby(["전출행정_시군구", "전출행정_동읍면", "전입년", "성별", "나이"]).size()}).reset_index()
    tmp3.sort_values(by=["전출행정_시군구","전출행정_동읍면"],axis=0,ascending=False)
    #tmp4 = pd.DataFrame(tmp3.pivot(index=["전출행정_시군구", "전출행정_동읍면", "성별", "나이"],columns="전입년",values="전출")).reset_index()
    tmp4 = tmp3.pivot_table(index=["전출행정_시군구", "전출행정_동읍면", "성별", "나이"], columns='전입년', values='전출').reset_index()
    tmp4 = tmp4.fillna(0)
    return tmp4

def pop_preprocess(raw_pop) :
    raw_pop["도"] = raw_pop.행정구역.str.split(" ").str[0]
    raw_pop["시"] = raw_pop.행정구역.str.split(" ").str[1]
    raw_pop["구"] = raw_pop.행정구역.str.split(" ").str[2]
    raw_pop["동"] = raw_pop.행정구역.str.split(" ").str[3]
    raw_pop.drop(['행정구역'], axis = 1, inplace = True)
    raw_pop = raw_pop.dropna(axis=0)
    is_tmp1 = raw_pop["동"].str.slice(0,1) != "("
    tmp1 = raw_pop[is_tmp1]
    tmp1["코드"] = tmp1.동.str.split("(").str[1]
    tmp1["동"] = tmp1.동.str.split("(").str[0]
    tmp2 = tmp1
    tmp2.loc[:,"기준년도"] = "pop_" + tmp2["기준년도"].map(str)
    ex_key = ["도","시","구","동","코드","기준년도"]
    in_key = []
    for key in tmp2.keys() :
        if key not in ex_key :
            in_key.append(key)

    tmp3 = pd.melt(tmp2,ex_key,in_key,"sex_age","pop")
    tmp3.astype({'pop':'int'}).dtypes
    ex_key = ["기준년도","pop"]
    in_key = []
    for key in tmp3.keys() :
        if key not in ex_key :
            in_key.append(key)

    tmp4 = pd.DataFrame(tmp3.pivot(columns="기준년도",values="pop"))
    tmp4 = pd.concat([tmp3,tmp4],axis=1)
    tmp4 = tmp4.drop(["기준년도","도","시","pop"],axis=1)
    tmp4["기타"] = tmp4.코드.str.split(")").str[1]
    tmp4["코드"] = tmp4.코드.str.split(")").str[0]
    tmp4 = tmp4.drop(['기타'], axis = 1)
    tmp4 = tmp4.fillna(0)
    tmp4 = tmp4.groupby(["구","동","코드","sex_age"]).sum().reset_index()
    return tmp4

def bd_pop_preprocess (raw_emd_death_birth) :
    raw_emd_death_birth.columns= ["std_year", "동","시군구", "성별", "birth", "death"]
    raw_emd_death_birth = raw_emd_death_birth.dropna(subset=['birth'])
    raw_emd_death = raw_emd_death_birth[["std_year", "동","시군구", "성별", "death"]]
    raw_emd_birth = raw_emd_death_birth[["std_year", "동", "성별", "birth"]]
    raw_emd_death["std_year"] = "사망_" + raw_emd_death["std_year"].map(str)
    raw_emd_birth["std_year"] = "출생_" + raw_emd_birth["std_year"].map(str)
    emd_death = raw_emd_death.groupby(["동", "시군구","성별", "std_year", "death"]).sum().reset_index()
    index = emd_death[emd_death['death']=="-"].index
    emd_death.drop(index,inplace=True)
    emd_death = emd_death.astype({'death':'float'})
    emd_death2 = pd.DataFrame(emd_death.pivot(columns="std_year",values="death"))
    emd_death = pd.concat([emd_death,emd_death2],axis=1)
    emd_death.drop(['death', 'std_year'], axis = 1, inplace = True)
    emd_birth = raw_emd_birth.groupby(["동", "성별", "std_year", "birth"]).sum().reset_index()
    index = emd_birth[emd_birth['birth']=="-"].index
    emd_birth.drop(index,inplace=True)
    emd_birth = emd_birth.astype({'birth':'float'})
    emd_birth2 = pd.DataFrame(emd_birth.pivot(columns="std_year",values="birth"))
    emd_birth = pd.concat([emd_birth,emd_birth2],axis=1)
    emd_birth.drop(['birth', 'std_year'], axis = 1, inplace = True)
    emd_death = emd_death.groupby(["동","시군구","성별"]).sum().reset_index()
    emd_birth = emd_birth.groupby(["동","성별"]).sum().reset_index()

    return emd_birth, emd_death

def move_pop() :
    raw_movein = pd.read_csv('/home/infra/user/ya/param/movein_pop_data.csv')
    raw_moveout = pd.read_csv('/home/infra/user/ya/param/moveout_pop_data.csv')
    movein_preprocess(raw_movein)
    moveout_preprocess(raw_moveout)
    moveout = pd.read_csv('/home/infra/user/ya/param/moveout.csv')
    movein = pd.read_csv('/home/infra/user/ya/param/movein.csv')
    ana_movein = age_factor_in(movein)
    ana_moveout = age_factor_out(moveout)

    move = pd.merge(ana_movein, ana_moveout, left_on = ["전입행정_시군구", "전입행정_동읍면", "성별", "나이"] ,right_on =["전출행정_시군구", "전출행정_동읍면", "성별", "나이"],how='inner')
    move.to_csv('/home/infra/user/ya/param/move.csv',mode="w",encoding='utf-8-sig')
    #move = move.fillna(0)
    move.drop(['전출행정_시군구', '전출행정_동읍면'], axis = 1, inplace = True)
    #after 2020 modify
    v1 = move.iloc[:,4] - move.iloc[:,10]
    v2 = move.iloc[:,5] - move.iloc[:,11]
    v3 = move.iloc[:,6] - move.iloc[:,12]
    v4 = move.iloc[:,7] - move.iloc[:,13]
    v5 = move.iloc[:,8] - move.iloc[:,14]
    v6 = move.iloc[:,9] - move.iloc[:,15]
    ana_move = pd.concat([move.iloc[:,0:4],v1,v2,v3,v4,v5,v6],axis=1)
    col_names = move.keys()
    new_col_names = []
    for col_name in col_names[4:10] :
        new_col_names.append("move_"+col_name[3:7])
    new_col_names = ["시군구","읍면동","성별","나이"] + new_col_names
    ana_move.columns = [new_col_names]
    ana_move = ana_move.fillna(0)
    ana_move.to_csv('/home/infra/user/ya/param/ana_move.csv',mode="w",encoding='utf-8-sig')

def pop() :
    raw_pop = pd.read_csv('/home/infra/user/ya/param/pop_data.csv')
    pop = pop_preprocess(raw_pop)
    pop["성별"] = pop.sex_age.str.split("\\_").str[1]
    pop["나이"] = pop.sex_age.str.split("\\_").str[3]
    index = pop[pop['성별']=='계'].index
    pop.drop(index, inplace=True)
    pop = pop.dropna(axis=0)
    pop = pop.drop("sex_age",axis=1)
    ex_key = ["구", "동", "코드", "성별","나이"]
    in_key = []
    for key in pop.keys() :
        if key not in ex_key :
            in_key.append(key)
    pop = pop[["구", "동", "코드", "성별","나이"]+in_key]
    pop = pop.sort_values(by=['구','동',"코드", "성별","나이"])

    pop.loc[pop["나이"]=="0~4세","나이"] = "0 - 4세"
    pop.loc[pop["나이"]=="5~9세","나이"] = "5 - 9세"
    pop.loc[pop["나이"]=="10~14세","나이"] = "10 - 14세"
    pop.loc[pop["나이"]=="15~19세","나이"] = "15 - 19세"
    pop.loc[pop["나이"]=="20~24세","나이"] = "20 - 24세"
    pop.loc[pop["나이"]=="25~29세","나이"] = "25 - 29세"
    pop.loc[pop["나이"]=="30~34세","나이"] = "30 - 34세"
    pop.loc[pop["나이"]=="35~39세","나이"] = "35 - 39세"
    pop.loc[pop["나이"]=="40~44세","나이"] = "40 - 44세"
    pop.loc[pop["나이"]=="45~49세","나이"] = "45 - 49세"
    pop.loc[pop["나이"]=="50~54세","나이"] = "50 - 54세"
    pop.loc[pop["나이"]=="55~59세","나이"] = "55 - 59세"
    pop.loc[pop["나이"]=="60~64세","나이"] = "60 - 64세"
    pop.loc[pop["나이"]=="65~69세","나이"] = "65 - 69세"
    pop.loc[pop["나이"]=="70~74세","나이"] = "70 - 74세"
    pop.loc[pop["나이"]=="75~79세","나이"] = "75 - 79세"
    pop.loc[pop["나이"]=="80~84세","나이"] = "80 - 84세"
    pop.loc[pop["나이"]=="85~89세","나이"] = "85 - 89세"
    pop.loc[pop["나이"]=="90~94세","나이"] = "90세 이상"
    pop.loc[pop["나이"]=="95~99세","나이"] = "90세 이상"
    pop.loc[pop["나이"]=="100세 이상","나이"] = "90세 이상"

    pop.loc[pop["성별"]=="남","성별"] = "남자"
    pop.loc[pop["성별"]=="여","성별"] = "여자"
    pop = pop.groupby(["구","동","코드","성별","나이"],as_index=False).sum()
    pop.to_csv('/home/infra/user/ya/param/pop.csv',mode="w",encoding='utf-8-sig')

def bd_pop() :
    raw_emd_death_birth = pd.read_csv('/home/infra/user/ya/param/bd_pop_data.csv')
    raw_sigungu_death = pd.read_csv('/home/infra/user/ya/param/death_pop_data.csv')
    emd_birth, emd_death = bd_pop_preprocess(raw_emd_death_birth)

    emd_birth.loc[emd_birth["성별"]=="남","성별"] = "남자"
    emd_birth.loc[emd_birth["성별"]=="여","성별"] = "여자"
    emd_death.loc[emd_death["성별"]=="남","성별"] = "남자"
    emd_death.loc[emd_death["성별"]=="여","성별"] = "여자"

    emd_birth.to_csv('/home/infra/user/ya/param/emd_birth.csv',mode="w",encoding='utf-8-sig')
    emd_death.to_csv('/home/infra/user/ya/param/emd_death.csv',mode="w",encoding='utf-8-sig')

    raw_sigungu_death["시점"] = "사망_" + raw_sigungu_death["시점"].map(str)
    raw_sigungu_death.columns = ["std_year","sigungu","sex","age","death","death_rate"]
    raw_sigungu_death = raw_sigungu_death[["std_year","sigungu","sex","age","death"]]
    raw_sigungu_death["death"] = raw_sigungu_death['death'].astype("int")
    raw_sigungu_death = pd.DataFrame(raw_sigungu_death.pivot_table(index=["sigungu","sex","age"],columns="std_year",values="death")).reset_index()
    raw_sigungu_death = raw_sigungu_death.fillna(0)
    in_col = ["sigungu","sex","age"]
    ex_col = []
    for key in raw_sigungu_death.keys() :
        if key not in in_col :
            ex_col.append(key)
    for key in ex_col :

        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="남자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="남자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="남자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="여지"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="여자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="권선구")&(raw_sigungu_death["sex"]=="여자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="남자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="남자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="남자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="여자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="여자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="영통구")&(raw_sigungu_death["sex"]=="여자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="남자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="남자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="남자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="여자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="여자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="장안구")&(raw_sigungu_death["sex"]=="여자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="남자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="남자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="남자"),key])
        raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="여자"),key] = int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="여자"),key]) + int(raw_sigungu_death.loc[(raw_sigungu_death["age"]=="1 - 4세")&(raw_sigungu_death["sigungu"]=="팔달구")&(raw_sigungu_death["sex"]=="여자"),key])


    raw_sigungu_death.loc[(raw_sigungu_death["age"]=="0세"),"age"] = "0 - 4세"
    raw_sigungu_death = raw_sigungu_death[raw_sigungu_death.age != "1 - 4세"]
    sigungu_sex_death = emd_death.groupby(["시군구","성별"]).sum().reset_index()
    raw_sigungu_death2 = pd.merge(emd_death, sigungu_sex_death, on=["성별","시군구"], how = 'inner')
    raw_sigungu_death2.to_csv('/home/infra/user/ya/param/raw_sigungu_death2.csv',mode="w",encoding='utf-8-sig')
    #after 2020data modify
    v1 = raw_sigungu_death2.iloc[:,3] / raw_sigungu_death2.iloc[:,9]
    v2 = raw_sigungu_death2.iloc[:,4] / raw_sigungu_death2.iloc[:,10]
    v3 = raw_sigungu_death2.iloc[:,5] / raw_sigungu_death2.iloc[:,11]
    v4 = raw_sigungu_death2.iloc[:,6] / raw_sigungu_death2.iloc[:,12]
    v5 = raw_sigungu_death2.iloc[:,7] / raw_sigungu_death2.iloc[:,13]
    v6 = raw_sigungu_death2.iloc[:,8] / raw_sigungu_death2.iloc[:,14]

    raw_sigungu_death3 = pd.concat([raw_sigungu_death2.iloc[:,0:3],v1,v2,v3,v4,v5,v6],axis=1)
    col_names = raw_sigungu_death.keys()
    col_names = ["사망_"+c[3:7]+"_rate" for c in col_names if c.startswith("사망")]
    raw_sigungu_death3.columns = ["dong","sigungu","sex"] + col_names
    emd_death1 = pd.merge(raw_sigungu_death,raw_sigungu_death3,on=["sex","sigungu"])
    emd_death1.to_csv('/home/infra/user/ya/param/emd_death1_0.csv',mode="w",encoding='utf-8-sig')
    emd_death1 = emd_death1.iloc[:,[0,9,1,2,3,4,5,6,7,8,10,11,12,13,14,15]]
    col_names = emd_death1.keys()
    emd_death1.to_csv('/home/infra/user/ya/param/emd_death1.csv',mode="w",encoding='utf-8-sig')
    v1 = emd_death1.iloc[:,4] * emd_death1.iloc[:,10]
    v2 = emd_death1.iloc[:,5] * emd_death1.iloc[:,11]
    v3 = emd_death1.iloc[:,6] * emd_death1.iloc[:,12]
    v4 = emd_death1.iloc[:,7] * emd_death1.iloc[:,13]
    v5 = emd_death1.iloc[:,8] * emd_death1.iloc[:,14]
    v6 = emd_death1.iloc[:,9] * emd_death1.iloc[:,15]

    emd_death2 = pd.concat([emd_death1.iloc[:,0:4],v1,v2,v3,v4,v5,v6],axis=1)
    emd_death2.columns = ["시군구", "동", "성별", "나이",col_names[4],col_names[5],col_names[6],col_names[7],col_names[8],col_names[9]]
    emd_death2.to_csv('/home/infra/user/ya/param/emd_death2.csv',mode="w",encoding='utf-8-sig')

def pre_cohort() :
    emd_birth = pd.read_csv('/home/infra/user/ya/param/emd_birth.csv')
    emd_death = pd.read_csv('/home/infra/user/ya/param/emd_death2.csv')
    ana_move = pd.read_csv('/home/infra/user/ya/param/ana_move.csv')
    pop = pd.read_csv('/home/infra/user/ya/param/pop.csv')

    cohort = pd.merge(pop, ana_move, left_on = ["동", "성별", "나이"], right_on = ["읍면동", "성별", "나이"], how = 'left')
    cohort.to_csv('/home/infra/user/ya/param/cohort2.csv')
    cohort.drop(['읍면동','시군구','Unnamed: 0_x','Unnamed: 0_y'], axis = 1, inplace = True)
    cohort["나이"] = pd.Categorical(cohort["나이"],categories=["0 - 4세", "5 - 9세", "10 - 14세", "15 - 19세",
                                                "20 - 24세", "25 - 29세", "30 - 34세", "35 - 39세",
                                                "40 - 44세", "45 - 49세", "50 - 54세", "55 - 59세",
                                                "60 - 64세", "65 - 69세", "70 - 74세", "75 - 79세",
                                                "80 - 84세", "85 - 89세", "90세 이상"],ordered=False)
    cohort.sort_values(by=["구","동","성별","나이"],axis=0,inplace=True)
    cohort = pd.merge(cohort, emd_death, left_on = ["동", "성별","나이"], right_on = ["동", "성별","나이"], how = 'left')
    cohort = pd.merge(cohort, emd_birth, left_on = ["동", "성별"], right_on = ["동", "성별"], how = 'left')
    cohort = cohort.dropna(subset=["나이"])
    cohort.to_csv('/home/infra/user/ya/param/cohort.csv',mode="w",encoding='utf-8-sig')

def dong():
    cohort = pd.read_csv('/home/infra/user/ya/param/cohort.csv')
    cohort = cohort.fillna(0)
    age_list = ['0 - 4세','5 - 9세','10 - 14세','15 - 19세','20 - 24세','25 - 29세','30 - 34세',
                '35 - 39세','40 - 44세','45 - 49세','50 - 54세','55 - 59세','60 - 64세',
               '65 - 69세','70 - 74세','75 - 79세','80 - 84세','85 - 89세','90세 이상']

    #금호동-> 금곡동&호매실동

    area_geumgok = 4095865.091
    area_homaesil = 4748119.852

    rate_geumgok = round(area_geumgok / (area_geumgok + area_homaesil), 2)
    rate_homaesil = round(area_homaesil / (area_geumgok + area_homaesil), 2)

    var1 = "pop_2015"
    var2 = "move_2015"
    var3 = "사망_2015"
    var4 = "출생_2015"
    var_list = [var1,var2,var3,var4]

    for age in age_list :
        for var in var_list :
            m = int(cohort.loc[(cohort['동']=='금호동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var])
            f = int(cohort.loc[(cohort['동']=='금호동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var])
            cohort.loc[(cohort['동']=='금곡동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_geumgok)
            cohort.loc[(cohort['동']=='금곡동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_geumgok)
            cohort.loc[(cohort['동']=='호매실동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_homaesil)
            cohort.loc[(cohort['동']=='호매실동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_homaesil)

    index = cohort[cohort['동']=='금호동'].index
    cohort.drop(index,inplace=True)

            
    #영통1동,영통2동,태장동 -> 영통1동,영통2동,영통3동,태장동
    area_yeongtong1 = 1601601.002
    area_yeongtong2 = 1323928.918
    area_yeongtong3 = 1697975.226
    area_taejang = 2499434.760

    rate_yeontong1 = round(area_yeongtong1 / (area_yeongtong1 + area_yeongtong2 + area_yeongtong3 + area_taejang), 2)
    rate_yeontong2 = round(area_yeongtong2 / (area_yeongtong1 + area_yeongtong2 + area_yeongtong3 + area_taejang), 2)
    rate_yeontong3 = round(area_yeongtong3 / (area_yeongtong1 + area_yeongtong2 + area_yeongtong3 + area_taejang), 2)
    rate_taejang = round(area_taejang / (area_yeongtong1 + area_yeongtong2 + area_yeongtong3 + area_taejang), 2)

    var1 = ["pop_2015","pop_2016"]
    var2 = ["move_2015","move_2016"]
    var3 = ["사망_2015","사망_2016"]
    var4 = ["출생_2015","출생_2016"]
    var_list = var1+var2+var3+var4
    for age in age_list :
        for var in var_list :
            m = int(cohort.loc[(cohort['동']=='영통1동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var]) + \
            int(cohort.loc[(cohort['동']=='영통2동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var]) + \
            int(cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var])
            f =  int(cohort.loc[(cohort['동']=='영통1동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var]) + \
            int(cohort.loc[(cohort['동']=='영통2동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var]) + \
            int(cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var])

            cohort.loc[(cohort['동']=='영통1동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_yeontong1)
            cohort.loc[(cohort['동']=='영통1동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_yeontong1)
            cohort.loc[(cohort['동']=='영통2동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_yeontong2)
            cohort.loc[(cohort['동']=='영통2동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_yeontong2)
            cohort.loc[(cohort['동']=='영통3동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_yeontong3)
            cohort.loc[(cohort['동']=='영통3동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_yeontong3)
            cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_taejang)
            cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_taejang)
    #태장동->망포1동,망포2동
    area_mangpo1 = 32055
    area_mangpo2 = 27670

    rate_mangpo1 = round(area_mangpo1 / (area_mangpo1 + area_mangpo2),2)
    rate_mangpo2 = round(area_mangpo2 / (area_mangpo1 + area_mangpo2),2)

    var1 = ["pop_2015","pop_2016","pop_2017","pop_2018"]
    var2 = ["move_2015","move_2016","move_2017","move_2018"]
    var3 = ["사망_2015","사망_2016","사망_2017","사망_2018"]
    var4 = ["출생_2015","출생_2016","출생_2017","출생_2018"]
    var_list = var1+var2+var3+var4
    
    for age in age_list :
        for var in var_list :
            m = int(cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var])
            f =  int(cohort.loc[(cohort['동']=='태장동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var])

            cohort.loc[(cohort['동']=='망포1동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_mangpo1)
            cohort.loc[(cohort['동']=='망포1동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_mangpo1)
            cohort.loc[(cohort['동']=='망포2동')&(cohort['성별']=='남자')&(cohort['나이'] ==age),var] = round(m*rate_mangpo2)
            cohort.loc[(cohort['동']=='망포2동')&(cohort['성별']=='여자')&(cohort['나이'] ==age),var] = round(f*rate_mangpo2)
    
    index = cohort[cohort['동']=='태장동'].index
    cohort.drop(index,inplace=True)
    cohort.to_csv('/home/infra/user/ya/param/cohort.csv',mode="w")

def female_rate() :
    pop = pd.read_csv('/home/infra/user/ya/param/cohort.csv')
    #가임기 여성 비율 
    index = pop[pop['성별']=="남자"].index
    pop.drop(index,inplace=True)
    gu_list = pop['구'].drop_duplicates()
    dong_dict = {}

    for gu in gu_list :
        temp_dict = {}
        dong_dict[gu] = {}
        gu_pop = pop[(pop['구'] == gu)]
        dong_list = gu_pop['동'].drop_duplicates()
        for dong in dong_list :
            temp = gu_pop[(gu_pop['동'] == dong)]
            temp = temp[['구','동','성별','나이','pop_2020']]
            female = int(temp.loc[temp['나이']=="20 - 24세","pop_2020"]) + int(temp.loc[temp['나이']=="25 - 29세","pop_2020"]) \
                    + int(temp.loc[temp['나이']=="30 - 34세","pop_2020"]) + int(temp.loc[temp['나이']=="35 - 39세","pop_2020"]) \
                        + int(temp.loc[temp['나이']=="40 - 44세","pop_2020"]) + int(temp.loc[temp['나이']=="45 - 49세","pop_2020"])
            temp_dict[dong] = female
        sum = 0
        for dong in temp_dict :
            sum += temp_dict[dong]
        for dong in temp_dict :
            temp_dict[dong] = temp_dict[dong]/sum
            dong_dict[gu][dong] = temp_dict[dong]
   
    return dong_dict

def sex_rate() :
    pop = pd.read_csv('/home/infra/user/ya/param/cohort.csv')
    #출생아 성별 비율
    sex_dict = {}
    temp_pop = pop[(pop['나이'] == '0 - 4세')]
    temp_pop = temp_pop[['성별','출생_2018','출생_2019','출생_2020']]
    temp_pop = temp_pop.groupby(['성별']).sum().reset_index()
    sum = temp_pop['출생_2018'].sum() + temp_pop['출생_2019'].sum() + temp_pop['출생_2020'].sum()
    female = int(temp_pop.loc[temp_pop['성별']=="여자","출생_2018"]) + int(temp_pop.loc[temp_pop['성별']=="여자","출생_2019"]) + int(temp_pop.loc[temp_pop['성별']=="여자","출생_2020"])
    male = int(temp_pop.loc[temp_pop['성별']=="남자","출생_2018"]) + int(temp_pop.loc[temp_pop['성별']=="남자","출생_2019"]) + int(temp_pop.loc[temp_pop['성별']=="남자","출생_2020"])
    sex_dict['여자'] = female/sum
    sex_dict['남자'] = male/sum
    return sex_dict

def pred_birth() :
    dong_dict = female_rate()
    sex_dict = sex_rate()
    birth_pred = pd.read_csv("/home/infra/user/ya/param/CHILD_GU_Pred.csv")
    birth_pred.columns = ['date','장안구','권선구','팔달구','영통구']
    birth_pred['year'] = birth_pred.date.str.slice(0,4)
    group = birth_pred.groupby(birth_pred['year']).sum().reset_index()
    year_list = group['year'].drop_duplicates()
    group = pd.melt(group,['year'],['장안구','권선구','팔달구','영통구'],"구","birth")
    group = group.pivot_table(index=["구"],columns='year',values='birth').reset_index()
    for gu in dong_dict :
        for dong in dong_dict[gu] :
            group.loc[group['구']==gu,'동'] = dong

    pred_birth = pd.DataFrame(columns=["동","성별","출생_2021","출생_2022","출생_2023","출생_2024","출생_2025"])
    for gu in dong_dict :
        for dong in dong_dict[gu] :
            male_출생_2021 = int(group.loc[group['구']==gu,"2021"]*dong_dict[gu][dong]*sex_dict["남자"])
            male_출생_2022 = int(group.loc[group['구']==gu,"2022"]*dong_dict[gu][dong]*sex_dict["남자"])
            male_출생_2023 = int(group.loc[group['구']==gu,"2023"]*dong_dict[gu][dong]*sex_dict["남자"])
            male_출생_2024 = int(group.loc[group['구']==gu,"2024"]*dong_dict[gu][dong]*sex_dict["남자"])
            male_출생_2025 = int(group.loc[group['구']==gu,"2025"]*dong_dict[gu][dong]*sex_dict["남자"])
            female_출생_2021 = int(group.loc[group['구']==gu,"2021"]*dong_dict[gu][dong]*sex_dict["여자"])
            female_출생_2022 = int(group.loc[group['구']==gu,"2022"]*dong_dict[gu][dong]*sex_dict["여자"])
            female_출생_2023 = int(group.loc[group['구']==gu,"2023"]*dong_dict[gu][dong]*sex_dict["여자"])
            female_출생_2024 = int(group.loc[group['구']==gu,"2024"]*dong_dict[gu][dong]*sex_dict["여자"])
            female_출생_2025 = int(group.loc[group['구']==gu,"2025"]*dong_dict[gu][dong]*sex_dict["여자"])
            pred_birth.loc[len(pred_birth)] = [dong,"여자",female_출생_2021,female_출생_2022,female_출생_2023,female_출생_2024,female_출생_2025]
            pred_birth.loc[len(pred_birth)] = [dong,"남자",male_출생_2021,male_출생_2022,male_출생_2023,male_출생_2024,male_출생_2025]
    
    cohort = pd.read_csv('/home/infra/user/ya/param/cohort.csv')
    cohort = pd.merge(cohort,pred_birth,on=["동","성별"])
    cohort.to_csv('/home/infra/user/ya/param/cohort.csv',mode="w")
    

def cohort(pred_year) :
    data = pd.read_csv('/home/infra/user/ya/param/cohort.csv')
    data.fillna(0)
    std_year = pred_year - 5
    std_year1 = "pop_" + str(std_year)
    pred_year = "pred_" + str(pred_year)
    move_year = "move_" + str(std_year)
    death_year = "사망_" + str(std_year)
    birth_year = "출생_" + str(std_year)

    data1 = data.copy()
    data1[birth_year] = np.nan
    data1 = data1[["구","동","코드","성별","나이",std_year1,move_year,death_year,birth_year]]
    gb = data1.groupby(["구","동","코드","성별"])
    gb_group = [gb.get_group(x) for x in gb.groups]
    after_gb = []
    concat_gb = pd.DataFrame()
    for gb_g in gb_group :
        gb_g[[std_year1,move_year,death_year]] = gb_g[[std_year1,move_year,death_year]].shift(1)
        after_gb.append(gb_g)
    for i in after_gb :
        concat_gb = pd.concat([concat_gb,i])
    concat_gb.loc[concat_gb["나이"]=="0 - 4세",std_year1] = 0
    concat_gb.loc[concat_gb["나이"]=="0 - 4세",move_year] = 0
    concat_gb.loc[concat_gb["나이"]=="0 - 4세",death_year] = 0
    concat_gb.loc[concat_gb["나이"]=="0 - 4세",birth_year] = data.loc[data["나이"]=="0 - 4세","출생_"+str(std_year+1)] + data.loc[data["나이"]=="0 - 4세","출생_"+str(std_year+2)]\
                                                            +data.loc[data["나이"]=="0 - 4세","출생_"+str(std_year+3)] + data.loc[data["나이"]=="0 - 4세","출생_"+str(std_year+4)]\
                                                            +data.loc[data["나이"]=="0 - 4세","출생_"+str(std_year+5)]
    concat_gb = concat_gb.fillna(0)
    concat_gb[pred_year] = round(concat_gb[std_year1] + concat_gb[move_year] - concat_gb[death_year] + concat_gb[birth_year])
    result = concat_gb[["구","동","성별","나이",pred_year]]
    return result

def iris():
    std_year = 2021
    info_path = '/home/infra/user/ya/info.json'
    info = json.load(open(info_path))
    host =  info['iris']['host']
    user =  info['iris']['user']
    password = info['iris']['password']
    database = info['iris']['database']
    conn = M6.Connection(host,user,password, Database=database, Timeout=60, Direct=False)
    cursor = conn.Cursor()
    pop_data(cursor,info,std_year)
    movein_pop_data(cursor,info,std_year)
    moveout_pop_data(cursor,info,std_year)
    bd_pop_data(cursor,info,std_year)
    death_pop_data(cursor,info,std_year)

if __name__ == "__main__" :
    try :
        parser = argparse.ArgumentParser()
        parser.add_argument("std_year", help="std_year : 2021")
        args = parser.parse_args()
        std_year = args.std_year
        #아이리스 연동
        #iris(std_year)
        #iris()
        #인구이동데이터 전처리
        move_pop()
        #인구데이터 전처리
        #pop()
        #출생사망데이터 전처리
        #bd_pop()
        #코호트 데이터 전처리
        #pre_cohort()
        #코호트 분석
        #dong()
        pred_birth()
        cohort0 = cohort(2020)
        cohort1 = cohort(2021)
        cohort2 = cohort(2022)
        cohort3 = cohort(2023)
        cohort4 = cohort(2024)
        cohort5 = cohort(2025)

        result = pd.merge(cohort0,cohort1,on=["구","동","성별","나이"])
        result = pd.merge(result,cohort2,on=["구","동","성별","나이"])
        result = pd.merge(result,cohort3,on=["구","동","성별","나이"])
        result = pd.merge(result,cohort4,on=["구","동","성별","나이"])
        result = pd.merge(result,cohort5,on=["구","동","성별","나이"])

        result.to_csv('/home/infra/user/ya/param/final.csv',mode="w",encoding='utf-8-sig')
        #for file in os.scandir('/home/infra/user/ya/param/') :
        #    os.remove(file.path)

    except Exception as e :
        print(traceback.format_exc())
