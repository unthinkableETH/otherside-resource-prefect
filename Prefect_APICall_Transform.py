import requests
import pandas as pd
import prefect
from prefect.blocks.system import Secret
from prefect import flow
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_upload
from io import BytesIO
import numpy as np
import math
import re
import httpx




secret_block = Secret.load("reservoir-api")

# Access the stored secret
api_key_reservoir=secret_block.get()

headers = {"x-api-key": api_key_reservoir}


def query_api(otherdeed_or_expanded):
    base_url="https://api.reservoir.tools/tokens/"
    price_base_url="floor/v1?collection="
    flag_base_url="ids/v1?collection="
    flag_end_url="&flagStatus=1&limit=10000"
    otherdeed_c="0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258"
    expanded_c="0x790B2cF29Ed4F310bf7641f013C65D4560d28371"
    def get_price(url):
        response_price=requests.get(url, headers=headers)
        data_price=response_price.json()
        data_token=data_price["tokens"]
        price_df=pd.DataFrame(data_token.items(), columns = ["PlotID","LowestPrice"])
        return(price_df)

    def get_flag(url):
        response_flag=requests.get(url, headers=headers)
        data_flag=response_flag.json()
        flag_list=data_flag["tokens"]
        return(flag_list)

    if otherdeed_or_expanded == "otherdeed":
        otherdeed_price_url=base_url+price_base_url+otherdeed_c
        otherdeed_flag_url=base_url+flag_base_url+otherdeed_c+flag_end_url
        price_df_final=get_price(otherdeed_price_url)
        flag_list_final=get_flag(otherdeed_flag_url)

    if otherdeed_or_expanded == "expanded":
        expanded_price_url=base_url+price_base_url+expanded_c
        expanded_flag_url=base_url+flag_base_url+expanded_c+flag_end_url
        price_df_final=get_price(expanded_price_url)
        flag_list_final=get_flag(expanded_flag_url)
    
    return(price_df_final,flag_list_final)


otherdeed_api=query_api("otherdeed")
expanded_api=query_api("expanded")



otherdeed_price_df=otherdeed_api[0]
otherdeed_flag_list=otherdeed_api[1]
expanded_price_df=expanded_api[0]
expanded_flag_list=expanded_api[1]



df_tp_with_flag_original=pd.merge(otherdeed_price_df,expanded_price_df, how="outer")
df_tp_with_flag=df_tp_with_flag_original.copy(deep=True)
df_tp_with_flag["PlotID"]=df_tp_with_flag['PlotID'].astype(int)


nfts_that_likely_just_burned=expanded_price_df[expanded_price_df["PlotID"].isin(otherdeed_flag_list)]
burn_list=list(nfts_that_likely_just_burned["PlotID"])
new_otherdeed_flag_list=list(set(otherdeed_flag_list).difference(burn_list))
final_flag_list=new_otherdeed_flag_list+expanded_flag_list
df_tp_without_flag_original=df_tp_with_flag[df_tp_with_flag["PlotID"].isin(final_flag_list) == False]
df_tp_without_flag=df_tp_without_flag_original.copy(deep=True)
df_tp_without_flag["PlotID"]=df_tp_without_flag['PlotID'].astype(int)




df_a=pd.read_csv("s3://otherside-resource/static-files/Amounts.csv") #Gives the amount needed for 1% or 2.5% or 5%% for each Resource
df_r=pd.read_csv("s3://otherside-resource/static-files/Resources.csv") #Gives what Resources each plot has
df_ur=pd.read_csv("s3://otherside-resource/static-files/UniqueResources.csv") #Short List of Unique Resources and their rarity rank
df_ur2=pd.read_csv("s3://otherside-resource/static-files/UniqueResources2.csv") #Short List of Unique Resources and their rarity rank
unique_list=df_ur["Resource"].tolist()
cols=["Northern Resource","Southern Resource","Western Resource","Eastern Resource"]


def da_of_otherside(df,percent_v,with_or_without):
    total_dict={}
    sort_dict={}
    rounded_dict={}
    zero_dict={}
    df_merge=pd.merge(df_r,df, on="PlotID",how="inner") #Merges new resource data with price data
    #Runs through Unique Resource list one by one, and makes a new dataframe if that resource is in any of the four slots of the NFT Plot
    #Puts that new dataframe in dictionary, so the result is, a dictionary full of keys of the different resources and
    #Values that are dataframes of any plot that has one of the resources, with ID, Resources, and Price data
    for x in unique_list:
        total_dict[x]=df_merge.loc[(df_merge["Northern Resource"] == x) | (df_merge["Southern Resource"] == x) | (df_merge["Eastern Resource"] == x) | (df_merge["Western Resource"] == x)]
    
  
    total_key_copy = tuple(total_dict.keys())
    for x in total_key_copy:
        if total_dict[x].empty:
            del total_dict[x] #if there is no plots with a particular resource for sale, delete that key/value from the dictionary
            continue
    for x in total_dict: 
        sort_dict[x]=total_dict[x].sort_values(by=['LowestPrice'],ignore_index=True) #sort by lowest price

    #This is to get rid of outliers using standard interquartile range (IQR) and the 1.5xIQR rule to remove high outliers
    #High outliers is common in NFTs as sometimes, those that dont really want to sell NFTs list as 420.69 eth or 69 eth
    for x in sort_dict:
        sort_dict[x]["Count"]=sort_dict[x][sort_dict[x][cols].astype(str)==x].count(axis=1)
        Q3=np.quantile(sort_dict[x]["LowestPrice"],0.75)
        Q1=np.quantile(sort_dict[x]["LowestPrice"],0.25)
        sort_dict[x]["IQRUpper"]=Q3+(1.5*(Q3-Q1))
        sort_dict[x]=sort_dict[x].loc[sort_dict[x]["LowestPrice"]<=sort_dict[x]["IQRUpper"]]
 
    for x in sort_dict:
        total=df_a[df_a['Unique List'].str.contains(x)]
        percent_value=percent_v/100
        amount=(int(total.iloc[0,1]))*percent_value
        amount_rup=math.ceil(amount)
        sort_dict[x]["Number of plots needed for "+str(percent_v)+"% control"]=amount_rup
        roundeddf=sort_dict[x].iloc[:amount_rup]
        num_rows=roundeddf.shape[0]
        if num_rows==amount_rup:
            sum_count=sum(roundeddf["Count"])
            if sum_count > num_rows:
                while sum_count > num_rows:
                    if sum_count == num_rows+1 and roundeddf["Count"].values[-1:]==2: 
                        break
                    if sum_count== num_rows+2 and roundeddf["Count"].values[-1:]==3:
                        break
                    if sum_count == num_rows +3 and roundeddf["Count"].values[-1:]==4:
                        break
                    else:
                        roundeddf=roundeddf.drop(roundeddf.index[-1], inplace=False)# drop plots to account for plots that have more than 1 of the same resource
                        sum_count=sum(roundeddf["Count"])
                rounded_dict[x]=roundeddf
            if sum_count <= num_rows: 
                rounded_dict[x]=roundeddf
    list_empty=[]
    zero_list=[]


    for x in rounded_dict:
        totalprice=rounded_dict[x]["LowestPrice"].sum(axis=0)
        totalprice_round=round(totalprice,2)
        plotids=','.join(rounded_dict[x]['PlotID'].astype(str))
        df_column = rounded_dict[x].columns[8]
        df_list=re.findall('Number of plots needed for (.*)%', df_column)
        df_percent= ' '.join([str(elem) for elem in df_list])
        totalprice_dict={"Resource" : x,"Total Price to Control "+df_percent+"% in ETH" : totalprice_round, "Number of Plots Needed for "+df_percent+"% Control" : rounded_dict[x].shape[0], "Plot IDs for Sale" : plotids}
        list_empty.append(totalprice_dict)
    
    list_sum_df=pd.DataFrame.from_records(list_empty)
    for x in unique_list:
        if list_sum_df.shape[0] == 74:
            zero_dict={"Resource" : "NA"}
            zero_list.append(zero_dict)
            break
        else:
            if x not in list_sum_df["Resource"].values:
                zero_dict={"Resource" : x}
                zero_list.append(zero_dict)
    zero_df=pd.DataFrame.from_records(zero_list)
    if list_sum_df.shape[0] == 74:
        zero_value= 0
        zero_df['Rarity'] = zero_value
        zero_df_sort=zero_df
    else:
        zero_df_merge=pd.merge(zero_df,df_ur2, on="Resource", how="inner")
        zero_df_sort=zero_df_merge.sort_values(by=["Rarity"])
   
    zero_df_csv_string=str(str(percent_v)+"%TotalPrice_"+with_or_without+"_zero.csv")
    
    aws_credentials = AwsCredentials.load("aws")

    csv_buffer = BytesIO()
    zero_df_sort.to_csv(csv_buffer, index=False)
    key = s3_upload(
        bucket="otherside-resource",
        key=zero_df_csv_string,
        data=csv_buffer.getvalue(),
        aws_credentials=aws_credentials,
        )

    
    df_column = list_sum_df.columns[1]
    df_list=re.findall('Price to Control (.*)%', df_column)
    df_percent= ' '.join([str(elem) for elem in df_list])
    df_sort=list_sum_df.sort_values(by=['Total Price to Control '+str(percent_v)+'% in ETH'])
    df_sort_rarity=pd.merge(df_sort,df_ur, on='Resource',how='inner')
    final_string=str(str(percent_v)+"%TotalPrice_"+with_or_without+".csv")
    
    csv_buffer2 = BytesIO()
    df_sort_rarity.to_csv(csv_buffer2, index=False)
    key2 = s3_upload(
        bucket="otherside-resource",
        key=final_string,
        data=csv_buffer2.getvalue(),
        aws_credentials=aws_credentials,
        )
    
@flow  
def csv_files_for_streamlit():
    da_of_otherside(df_tp_with_flag,1,"with_flag")
    da_of_otherside(df_tp_with_flag,2,"with_flag")
    da_of_otherside(df_tp_with_flag,3,"with_flag")
    da_of_otherside(df_tp_without_flag,1,"without_flag")
    da_of_otherside(df_tp_without_flag,2,"without_flag")
    da_of_otherside(df_tp_without_flag,3,"without_flag")

if __name__ == "__main__":
    csv_files_for_streamlit.from_source(
        source="https://github.com/unthinkableETH/Otherside-Resource.git",
        entrypoint="Prefect_APICall_Transform.py:csv_files_for_streamlit",

    ).deploy(
        name="otherside-elt", 
        work_pool_name="my-managed-pool", 
        job_variables={"pip_packages": ["requests","pandas", "prefect","io","numpy","math","re", "prefect-aws","boto3","s3fs","botocore","httpx"],"PREFECT_LOGGING_LEVEL":"DEBUG"},
        cron="0 * * * *",
    )