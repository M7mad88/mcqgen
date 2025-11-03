import time
from typing import Any, Dict, List, Tuple
import sys
from datetime import datetime, timedelta,date
import datetime 
import numpy as np
import pandas as pd
import pickle
import logging 
import argparse
import yaml 
import teradatasql as td
import json 
import os 
from scipy.stats import boxcox 
from scipy.special import inv_boxcox




logger = logging.getLogger(__file__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=os.path.join(
        "/data/u_AA/throughput_opt/logs",
        f"logs_run{datetime.datetime.now():%Y-%m-%d}_new.txt",
    ),
    filemode='w'
)

def inference(df,configuration):
    df_copy=  df.drop(['CELL_NAME','DATE_ID'],axis=1)
    logger.info("starting inference ")
    df_copy.DL_TRAFFIC_GB=df.Dl_traffic.values
    df_copy.DL_PRB_UTI=df.DL_PRB_UTI_t.values
    df_copy.AVG_ACTIVE_UE_DL=df.AVG_ACTIVE_UE_DL_t.values
    df_copy.UL_TRAFFIC_GB=df.UL_TRAFFIC_GB_t.values
    
    df_copy.drop(['Dl_traffic'],axis=1,inplace=True)
    logger.info("model loaded ")
    model = pickle.load(open(configuration['model'],'rb'))
    frequency = pickle.load(open(configuration['frequency'], 'rb'))
    
    categorical = df_copy.select_dtypes(['object'])
    numerical = df_copy.select_dtypes(['float64','int64'])
    
    band_mapping={'T':1,'D':0}
    mimo_mapping ={'Rank4':1,'Rank2':0}
    
    encoded_categorical=pd.DataFrame()
    encoded_categorical['band']=categorical['band'].map(frequency)
    encoded_categorical['band_T'] = categorical['band_letter'].map(band_mapping)
    encoded_categorical['MIMO_Rank4'] = categorical['MAXMIMORANKPARA'].map(mimo_mapping)
    
    logger.info("mapped features ")
    
    features= pd.concat([numerical , encoded_categorical ],axis=1)
    columns =  ['DL_TRAFFIC_GB',
                'UL_TRAFFIC_GB',
                'DL_PRB_UTI',
                'CCE_UTI',
                'VOLTE_DL_TRAFFIC_ERL',
                'VOLTE_DL_TRAFFIC_MB',
                'VOLTE_UL_TRAFFIC_MB',
                'AVG_ACTIVE_UE_DL',
                'PRB_DL_AVAILABLE',
                'CQI_AVERAGE',
                'MIMO_RANK_2_PERCENTAGE',
                'MIMO_RANK_3_PERCENTAGE',
                'MIMO_RANK_4_PERCENTAGE',
                'REFERENCESIGNALPWR',
                'MAXIMUM_TRANSMIT_POWER',
                'band',
                'band_T',
                'MIMO_Rank4']
    
    entered_features= features[columns]
    
    entered_features=entered_features.reindex(columns=columns)
    
    y_predict = model.predict(entered_features)
    
    logger.info("done predicting")
    df['predicted_throughput']=y_predict
    
    return df 

def tiering(x):
    if x >= 60 and x <70 :
        return "60-70"
    elif x >= 70 and x <80 :
        return "70-80"
    elif x >= 80 and x <90 :
        return "80-90"
    elif x >= 90 and x <=100 :
        return "90-100"
    
# def stats_model(features,group,tier,configuration):
#     logger.info("starting statistical inference")
#     transformed_df=pd.DataFrame()
#     transformed_df['DL_TRAFFIC_GB'], lambda_traffic = boxcox(features.Dl_traffic) 
#     transformed_df['DL_PRB_UTI'],lambda_PRB_UTI= boxcox(features.DL_PRB_UTI) 
#     transformed_df['AVG_ACTIVE_UE_DL'],lambda_AVG_UE= boxcox(features.AVG_ACTIVE_UE_DL) 
#     transformed_df['UL_TRAFFIC_GB'],lambda_UL_traffic= boxcox(features.UL_TRAFFIC_GB) 
#     logger.info("Transformed features")
    
#     stats_model= pickle.load(open(configuration['stats_param'],'rb'))
#     for col in transformed_df.columns:
#         if col != 'DL_TRAFFIC_GB':
#             logger.info(f"updating {col} from equation")
#             transformed_df[col] = stats_model[group][tier][col][0]+stats_model[group][tier][col][1]*transformed_df['DL_TRAFFIC_GB']
            
#     logger.info("de-transforming features")
#     transformed_df['DL_PRB_UTI_t']= inv_boxcox(transformed_df.DL_PRB_UTI,lambda_PRB_UTI)
#     transformed_df['AVG_ACTIVE_UE_DL_t']= inv_boxcox(transformed_df.AVG_ACTIVE_UE_DL,lambda_AVG_UE)
#     transformed_df['UL_TRAFFIC_GB_t']= inv_boxcox(transformed_df.UL_TRAFFIC_GB,lambda_UL_traffic)
#     logger.info("finished statistical inference")
    
#     return transformed_df

def stats_model(features,group,tier,configuration):
    logger.info("starting statistical inference")
    
    transformed_df=pd.DataFrame()
    lambda_c=pickle.load(open('/data/u_AA/throughput_opt/lambda_coeff.pickle','rb'))
    transformed_df['DL_TRAFFIC_GB']= boxcox(features.Dl_traffic,lambda_c[group][tier][0]) 
    transformed_df['UL_TRAFFIC_GB']= boxcox(features.UL_TRAFFIC_GB,lambda_c[group][tier][1]) 
    transformed_df['DL_PRB_UTI']= boxcox(features.DL_PRB_UTI,lambda_c[group][tier][2]) 
    transformed_df['AVG_ACTIVE_UE_DL']= boxcox(features.AVG_ACTIVE_UE_DL,lambda_c[group][tier][3]) 
 
    logger.info("Transformed features")
    # logger.info("traffic lamda ",lambda_traffic)
    # logger.info("UL traffic lamda",lambda_UL_traffic)
    # logger.info("PRB UTI lamda ",lambda_PRB_UTI)
    # logger.info("AVG UE lamda ",lambda_AVG_UE)
    # logger.info()
    stats_model= pickle.load(open(configuration['stats_param'],'rb'))
    for col in transformed_df.columns:
        if col != 'DL_TRAFFIC_GB':
            logger.info(f"updating {col} from equation")
            transformed_df[col] = stats_model[group][tier][col][0]+stats_model[group][tier][col][1]*transformed_df['DL_TRAFFIC_GB']
            
    logger.info("de-transforming features")
    transformed_df['DL_PRB_UTI_t']= inv_boxcox(transformed_df.DL_PRB_UTI,lambda_c[group][tier][2])
    transformed_df['AVG_ACTIVE_UE_DL_t']= inv_boxcox(transformed_df.AVG_ACTIVE_UE_DL,lambda_c[group][tier][3])
    transformed_df['UL_TRAFFIC_GB_t']= inv_boxcox(transformed_df.UL_TRAFFIC_GB,lambda_c[group][tier][1])
    logger.info("finished statistical inference")
    
    return transformed_df    


def Simulation( configuration: Dict) -> None:
    
    output=pd.DataFrame()
    compare=pd.DataFrame()
    
    transformed_df=pd.DataFrame()
    logger.info("data fetched")
    cells_params_30D= pd.read_parquet(configuration['datapath'])
   
    cells_params_30D= cells_params_30D[~(cells_params_30D.DL_TRAFFIC_GB <= 0.1)]
    cells_params_30D= cells_params_30D[~(cells_params_30D.PRB_DL_AVAILABLE > 100)]
    cells_params_30D= cells_params_30D[~(cells_params_30D.DL_PRB_UTI== 0)]
    
    logger.info("getting parameters lookup")
    
    # merged_lookup= pd.read_parquet(configuration['lookup'])


    # cells_params_30D= pd.merge(all_df_30D,merged_lookup[['CELL_NAME','MAXMIMORANKPARA','REFERENCESIGNALPWR','MAXIMUM_TRANSMIT_POWER']],how='left',on='CELL_NAME')
    logger.info("lookup merged with dataframe")
   
    cells_params_30D.dropna(subset=['REFERENCESIGNALPWR','MAXIMUM_TRANSMIT_POWER'],inplace=True)
    cells_params_30D.MAXMIMORANKPARA.fillna('Rank2',inplace=True)
    
    cells_params_30D=cells_params_30D[cells_params_30D['CQI_AVERAGE'].notna()]
    cells_params_30D=cells_params_30D[cells_params_30D['AVG_ACTIVE_UE_DL'].notna()]
    cells_params_30D=cells_params_30D[cells_params_30D['PRB_DL_AVAILABLE'].notna()]
    cells_params_30D=cells_params_30D[~((cells_params_30D['REFERENCESIGNALPWR']<0 )|(cells_params_30D['MAXIMUM_TRANSMIT_POWER']==65535))]
    
    cells_params_30D=cells_params_30D[~(cells_params_30D.DL_THROUGHPUT<=configuration['threshold'])]
    logger.info("Data filtered")
    
    cells_params_30D.drop(['HOUR_ID','UL_THROUGHPUT'],axis=1,inplace=True)
    logger.info(cells_params_30D.columns)
    
    logger.info("prepared features")
    cells_params_30D['group']= cells_params_30D['band'].apply(lambda x: x if x in ['T8','T4'] else 'D')
                    
    cells_params_30D["tier"]= cells_params_30D.PRB_DL_AVAILABLE.apply(tiering) 
    
    

    
    
    for (grouping, tier), group in cells_params_30D.groupby(['group', 'tier']):
        logger.info(f"starting group {grouping} {tier}")
        group.reset_index(drop=True, inplace=True)
        final=pd.DataFrame()
        counter = 1
        while counter<=20 :  
            logger.info(f"starting group {grouping} {tier} iteration {counter}")
            group['Dl_traffic'] = group['DL_TRAFFIC_GB'] + (configuration['step']*counter)
            group['percentage']=(group['Dl_traffic']-group['DL_TRAFFIC_GB'])/group['DL_TRAFFIC_GB']
            logger.info(f"scaled traffic ")
            
            transformed_df= stats_model(group[['Dl_traffic','DL_PRB_UTI','AVG_ACTIVE_UE_DL','UL_TRAFFIC_GB']],grouping,tier,configuration)
            
            group['DL_PRB_UTI_t'] = transformed_df['DL_PRB_UTI_t'].values
            group['AVG_ACTIVE_UE_DL_t'] = transformed_df['AVG_ACTIVE_UE_DL_t'].values
            group['UL_TRAFFIC_GB_t'] = transformed_df['UL_TRAFFIC_GB_t'].values
            
            logger.info(f"group cells shape : {group.shape}") 

            logger.info("finished scaling")    
            output = inference(group,configuration)  
            
            logger.info(f"output columns  : {output.columns}") 
            logger.info(f"output shape  : {output.shape}") 
            logger.info("finished predicting")     
            compare =output[output['predicted_throughput']<=configuration['threshold']][['CELL_NAME','DATE_ID']]
            
            logger.info(f"compare shape: {compare.shape}")
            logger.info("added cells to final df")    
            compare['new_traffic'] = output[output['predicted_throughput']<=configuration['threshold']]['Dl_traffic']-configuration['step'] 
            logger.info(f"compare shape: {compare.shape}")
            logger.info("added new traffic to final df")   
            compare = pd.merge(compare,output[['CELL_NAME','DATE_ID','DL_TRAFFIC_GB','DL_THROUGHPUT']],on=['CELL_NAME','DATE_ID'],how='inner')
            
            logger.info("added original traffic to final df")  
            logger.info(f"new_group shape: {output[~(output['predicted_throughput']<=configuration['threshold'])].shape}")  
            group=output[~(output['predicted_throughput']<=configuration['threshold'])]
            
            group.drop(['predicted_throughput'],axis=1,inplace=True)
            
            #compare[f'traffic_{counter}']=output['predicted_throughput']
            logger.info(f"output cells shape : {compare.shape}") 
            if len(compare)!=0:
                logger.info("saving the output")    
                final = pd.concat([final,compare],axis=0)
                del compare
            counter +=1 
            
        final.to_parquet(os.path.join(configuration['output'],f"cells_{grouping}_{tier}_7MB_v6.parquet"))    
        del final 
        
    
def parse_setup_args():
    

    parser = argparse.ArgumentParser(
        description="Execute the prediction script", add_help=False
    )


    
    parser.add_argument(
        "--configuration",
        type=str,
        required=True,
        help="location of the config yml file",
    )
   
    args = parser.parse_args()

    configuration = yaml.safe_load(open(args.configuration))

    return configuration




def main():
    configuration = parse_setup_args()
    #previous_period=period - timedelta(days=1) # date-1 

    logger.info(
        "Start Model"
    )
    
    
    try:

        Simulation(
        
        configuration=configuration,
        
    )

    # _log_to_message_log_database(
    #     conn=conn, message="Success", configuration=configuration
    # )



    except Exception as e:
        # Call bash messaging script
        logger.error(f"Error during execution: {e}")

        # TODO: Consider more informative error messages?
        
        # _log_to_message_log_database(
        #     conn=conn, message="Error", configuration=configuration
        # )

    logger.info("END Model")





def _log_to_message_log_database(
    conn: td.TeradataConnection,
    message: str,
    configuration: Dict,
):
    cur = conn.cursor()

    message_table = configuration["config"].get(
        "message_table", "anstagedb.Message_Log"
    )

    logger.info(f"Writing {message.lower()} message to database {message_table}")

    cur.execute(
        "insert into " + message_table + " (?, ?, ?)",
        [[datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message,configuration["config"].get("model_name")]],
    )





if __name__ == "__main__":
    main()









    
    
    
    
    
    




