import pandas as pd
import pandas as pd
from sklearn.metrics import mean_squared_error,mean_squared_log_error
import numpy as np

### RMSLE
def rmsle(y_test, y_pred):
    msle = mean_squared_log_error(y_test, y_pred)
    return np.sqrt(msle)

### MAPE
def mape(y_test, y_pred):
        return np.mean(np.abs((y_test - y_pred) / y_test)) * 100

### SMAPE
def smape(y_test, y_pred):
    return 1/len(y_test) * np.sum(2 * np.abs(y_pred-y_test) / (np.abs(y_test) + np.abs(y_pred))*100)

### RMSE
def rmse(y_test, y_pred):
        mse = mean_squared_error(y_test, y_pred)
        return np.sqrt(mse)

def kosis_pop_preprocess(raw_pop):
    raw_pop["기준년도"] = "pop_" + raw_pop["기준년도"].map(str)
    ex_key = ["동","기준년도"]
    in_key = []
    for key in raw_pop.keys() :
        if key not in ex_key :
            in_key.append(key)
    raw_pop = pd.melt(raw_pop,ex_key,in_key,"sex_age","pop")
    raw_pop.astype({'pop':'int'}).dtypes
    ex_key = ["기준년도","pop"]
    in_key = []
    for key in raw_pop.keys() :
        if key not in ex_key :
            in_key.append(key)
    tmp = pd.DataFrame(raw_pop.pivot(columns="기준년도",values="pop"))
    tmp = pd.concat([raw_pop,tmp],axis=1)
    tmp = tmp.drop(["기준년도","pop"],axis=1)
    return tmp

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
    return tmp4


def kosis_pop():
    pop = kosis_pop_preprocess(kosis)
    pop["성별"] = pop.sex_age.str.split("\\_").str[1]
    pop["나이"] = pop.sex_age.str.split("\\_").str[2]
    pop.drop("sex_age",axis=1,inplace=True)
    pop["성별"] = pop.성별.str[0:2]
    pop.loc[pop["나이"]=="90 - 94세","나이"] = "90세 이상"
    pop.loc[pop["나이"]=="95 - 99세","나이"] = "90세 이상"
    pop.loc[pop["나이"]=="100+","나이"] = "90세 이상"
    pop = pop.groupby(["동","성별","나이"],as_index=False).sum()
    pop.to_csv('/home/infra/user/ya/param/kosis_pop.csv')
    return pop
def pop() :
    pop = pop_preprocess(raw_pop)
    pop["성별"] = pop.sex_age.str.split("\\_").str[1]
    pop["나이"] = pop.sex_age.str.split("\\_").str[3]
    index = pop[pop['성별']=='거주자'].index
    pop.drop(index, inplace=True)
    index = pop[pop["나이"]=='연령구간인구수'].index
    pop.drop(index, inplace=True)
    index = pop[pop["나이"]=='총인구수'].index
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
    pop.to_csv('/home/infra/user/ya/param/pop2.csv',mode="w")
    return pop


if __name__ == "__main__" :
    raw_pop = pd.read_csv("/home/infra/user/ya/param/202110_202110_연령별인구현황_월간.csv")
    raw_pop = raw_pop.drop([raw_pop.columns[1],raw_pop.columns[2],raw_pop.columns[24],raw_pop.columns[25],raw_pop.columns[47],raw_pop.columns[48]], axis=1)
    actual = pop()
    #result = pd.read_csv('/home/ya/data/result.csv')
    #result2 = pd.merge(result,actual,on=['구','동','성별','나이'])
    #result2 = result2[["pred_2021","pop_2021"]]
    #result2 = result2.astype('int')
    #kosis = pd.read_csv('/home/infra/user/ya/param/kosis_pop2020.csv')
    #actual = kosis_pop()
    final = pd.read_csv('/home/infra/user/ya/param/final.csv')
    #actual = pd.read_csv('/home/infra/user/ya/param/pop.csv')
    result = pd.merge(final,actual,on=['동','성별','나이'])
    result.to_csv('/home/infra/user/ya/param/ve_re.csv')
    #result.drop(index,inplace=True)
    a = result["pred_2021"].astype('int')
    b = result["pop_2021"].astype('int')
    mape_result = mape(b, a)
    smape_result = smape(b, a)
    rmse_result = rmse(b, a)
    rmsle_result = rmsle(b,a) 
    print(f'mape : {mape_result} smape : {smape_result} rmse : {rmse_result} rmsle : {rmsle_result}')

