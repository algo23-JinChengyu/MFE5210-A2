import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import re
import cx_Oracle
import warnings
warnings.filterwarnings("ignore")
intern = cx_Oracle.makedsn('119.147.213.115', '1522', service_name='wktzorcl')
today=int(str(dt.date.today())[:4]+str(dt.date.today())[5:7]+str(dt.date.today())[8:])

#从数据库中获取股权激励预案数据和交易日数据
def GetData(user,password):
    '''
    user为数据库账号,password为数据库密码
    输出的两个表格分别是:
    1.股权激励公告数据，包含三列：股票代码、股权激励实施条件文本、股权激励预案公告日期
    2.交易日数据: 包含一列: A股交易日日期
    '''
    conn = cx_Oracle.connect(user, password, dsn=intern )
    cur=conn.cursor()
    cur.execute('select S_INFO_WINDCODE,S_INC_OPTEXESPECIALCONDITION,PREPLAN_ANN_DATE from WKWD_SYNC.AShareincdescription order by PREPLAN_ANN_DATE')
    plan_text=pd.DataFrame(cur.fetchall())
    plan_text.columns=['code','text','plandate']
    plan_text['plandate']=plan_text['plandate'].apply(lambda x: int(x))
    cur.execute("select Trade_days from WKWD_SYNC.ASHARECALENDARZL where S_INFO_EXCHMARKET='SSE' and Trade_days<={} order by TRADE_DAYS".format(today))
    fulldt=pd.DataFrame(cur.fetchall())
    fulldt.columns=['plandate']
    fulldt['plandate']=fulldt['plandate'].apply(lambda x: int(x))
    cur.close()
    conn.close()
    plan_text=plan_text[plan_text['text'].apply(lambda x: x!=None)].drop_duplicates().reset_index(drop=True)
    fulldt=fulldt.drop_duplicates().reset_index(drop=True)
    return plan_text,fulldt

#提取目标年份和目标净利润增速
def DataMine(text):
    ''''
    输入变量为股权激励实施条件文本
    输出股权激励目标年份和目标净利润增速的字符串, 比如：
        公司股权激励实施条件为2018年净利润增速不低于30%，则输出结果为：'2018,30' (但基准年并不一定是2017年,后续回筛选基准年份)
    如果未能提取出目标数据则输出为0
    '''
    if '利润' not in text:
        return 0#'无利润字样'
    else:
        text_parts=re.split('[，。：:,;；<>]',text)
        sentence_list=[k for k in text_parts if (('利润' in k)&('%' in k)&('分别不低于' in k))|(('利润' in k)&('%' in k)&('分别达到或超过' in k))]
        if len(sentence_list)!=0:
            sentence=sentence_list[0]
            if '分别不低于' in sentence:
                sentence_parts=re.split('分别不低于',sentence)
            else:
                sentence_parts=re.split('分别达到或超过',sentence)
            year_sentence=sentence_parts[0]
            rate_sentence=sentence_parts[1]
            year=re.search('(\d+)',year_sentence)
            rate=re.search('(\d+)',rate_sentence)
            return year[0]+','+rate[0]
        else:
            text_parts=re.split('[，。：:,;；、<>]',text)
            sentence_list=[k for k in text_parts if ('利润' in k)&('%' in k)|('利润' in k)&('％' in k)]
            if len(sentence_list)!=0:
                sentence=sentence_list[0]
                #匹配目标年份和增速字段
                if '较' in sentence:
                    if '％' in sentence:
                        result=re.search(r'(\d+).*较.*利润.*不低于(.*)％',sentence)
                        if result==None:
                            result=re.search(r'(\d+).*利润.*较.*不低于(.*)％',sentence)
                    else:
                        result=re.search(r'(\d+).*较.*利润.*不低于(.*)%',sentence)
                        if result==None:
                            result=re.search(r'(\d+).*利润.*较.*不低于(.*)%',sentence)
                elif '相对于' in sentence:
                    if '％' in sentence:
                        result=re.search(r'(\d+).*相对于.*利润.*不低于(.*)％',sentence)
                        if result==None:
                            result=re.search(r'(\d+).*利润.*相对于.*不低于(.*)％',sentence)
                    else:
                        result=re.search(r'(\d+).*相对于.*利润.*不低于(.*)%',sentence)
                        if result==None:
                            result=re.search(r'(\d+).*利润.*相对于.*不低于(.*)%',sentence)
                else:
                    if '％' in sentence:
                        result=re.search(r'(\d+).*利润.*不低于(.*)％',sentence)
                    else:
                        result=re.search(r'(\d+).*利润.*不低于(.*)%',sentence)
                if result != None:
                    year=result.group(1)
                    rate=result.group(2).strip()
                    if (len(year)==4)&(year[:2]=='20')&(len(set(rate).intersection(['.','0','1','2','3','4','5','6','7','8','9']))==len(set(rate))):
                        return (year+','+rate)
                    else:
                        return 0 #'年份匹配失败'
                else:
                    return 0 #'未匹配成功'
            else:
                return 0 #'有‘利润’但无‘%’字段'

def Yearcheck(text): 
     '''
     输入变量为股权激励实施条件文本
     输出变量为目标净利润增速的基准年, 比如公司目标为2020年净利润增速相比2019年增速不低于30%, 则输出为int格式:2018
     如果基准年份不是具体某一年, 而是过去几年的平均值等说法, 则输出0
     如果未提取到基准年份, 则默认基准年份是去年, 输出1
     '''
     if '利润' not in text:
        return 0#'无利润字样'
     else:
        text_parts=re.split('[，。：:,;；、<>]',text)
        sentence_list=[k for k in text_parts if ('利润' in k)&('%' in k)|('利润' in k)&('％' in k)]
        if len(sentence_list)!=0:
            sentence=sentence_list[0]
            #匹配目标年份和增速字段
            try:
                if '%' in sentence:
                    result=re.search(r'(.*)年.*较(.*)年.*利润.*不低于(.*)%',sentence)
                    if result==None:
                        result=re.search(r'(.*)年.*利润.*较(.*)年.*不低于(.*)%',sentence)
                else:
                    result=re.search(r'(.*)年.*较(.*)年.*利润.*不低于(.*)％',sentence)
                    if result==None:
                        result=re.search(r'(.*)年.*利润.*较(.*)年.*不低于(.*)％',sentence)
                year=result.group(2).strip('公司 于年')
            except:
                sentence_list=[k for k in text_parts if ('基' in k)]
                try:
                    sentence=sentence_list[0]
                    result=re.search('以(.*)年.*基.*',sentence)
                    year=result.group(1).strip('公司 年')
                except:
                    sentence_list=[k for k in text_parts if ('较' in k)]
                    try:
                        sentence=sentence_list[0]
                        result=re.search(r'较(.*)年.*',sentence)
                        year=result.group(1).strip('公司 年')
                    except:
                        return 1
            if (len(year)==4)&(year[:2]=='20'):
                return int(year)
            else:
                return 0
        else:
            return 0

#按条件获取持仓表函数
def TradeStocks(user,password,freq=5,days=80,highabove=40,minhold=10):
    '''
    freq=5表示每5天调一次仓
    days=80表示以过去80个交易日内发布股权激励预案公告的公司为待筛选股票池
    highabove=40表示选取目标净利润增速不低于30%的股票
    minhold=10表示持有至少10只股票,当满足目标净利润增速不低于highabove=40的股票不足10只时则买入净利润增速最大的10只股票
    输出用于回测的持仓表
    '''
    #获取原始数据
    plan_copy,fulldt=GetData(user,password)
    #提取出公告中目标净利润增速、目标年份、基准年份
    plan_copy['keytext']=plan_copy['text'].apply(DataMine)
    plan_copy['planyear']=plan_copy['plandate'].apply(lambda x: int(str(x)[:4]))
    plan_copy['baseyear']=plan_copy['text'].apply(Yearcheck)
    plan_copy['targetyear']=plan_copy.apply(lambda x: x['planyear'] if x['keytext']==0 else int(x['keytext'][:x['keytext'].index(',')]),axis=1)
    plan_copy['baseyear']=plan_copy.apply(lambda x: x['targetyear']-1 if x['baseyear']==1 else x['baseyear'] ,axis=1)
    plan_copy['increase']=plan_copy.apply(lambda x: 0 if (x['keytext']==0)|(x['baseyear']==0) else ((1+float(x['keytext'][x['keytext'].index(',')+1:])/100)**(1/max(1,x['targetyear']-x['baseyear']))-1)*100,axis=1)
    plan=plan_copy[plan_copy['targetyear']==plan_copy['planyear']].reset_index(drop=True)[['code','plandate','increase']]
    plan.drop_duplicates(inplace=True)
    plan=plan.reset_index(drop=True)
    #获取所有调仓日
    tradingday=fulldt[::freq]['plandate'].to_list()
    #选出每个调仓日回看days=80个交易日内符合筛选条件的股票
    holdings=pd.DataFrame()
    for day in tradingday:
        period=plan[(plan['plandate']<day)&(plan['plandate']>=max(0,tradingday[tradingday.index(day)-days//freq]))] 
        period['plandate']=day
        period['increase_rank']=period['increase'].rank(method='first')
        period['second_rank']=period['increase_rank'].rank(method='first',ascending=False)
        period=period[(period.increase>=highabove)|(period.second_rank<=minhold)]
        holdings=pd.concat([holdings,period],axis=0)
    #设置权重，组合内股票等权
    holdings=pd.merge(holdings[['code','plandate']],holdings.groupby('plandate').count(),on='plandate',how='left')
    holdings['weight']=holdings.groupby('plandate')['code_y'].apply(lambda x: 1/x)
    holdings.rename(columns={'code_x':'windcode','plandate':'date'},inplace=True)
    holdings=holdings[['windcode','date','weight']].reset_index(drop=True)
    return holdings
