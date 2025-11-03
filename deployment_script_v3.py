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
    features = ['DL_THROUGHPUT','DL_PRB_UTI','REFERENCESIGNALPWR','MAXIMUM_TRANSMIT_POWER','MIMO_RANK_2_PERCENTAGE','MIMO_RANK_3_PERCENTAGE','MIMO_RANK_4_PERCENTAGE']
    #features = ['DL_THROUGHPUT','DL_PRB_UTI']
    poly = pickle.load(open("/data/u_AA/throughput_opt/transoformer_const.pickle",'rb'))
    model = pickle.load(open("/data/u_AA/throughput_opt/traffic_model_const.pickle",'rb'))
    n = 7
    X = poly.transform(np.array(df[features]).reshape(-1, n)) 
    y_pred = model.predict(X)
    df['pred_traffic']=y_pred
    return df 


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
    
    
    #cells_params_30D=cells_params_30D[~(cells_params_30D.DL_THROUGHPUT<=configuration['threshold'])]
    logger.info("Data filtered")
    
    
    cells_params_30D.drop(['HOUR_ID','UL_THROUGHPUT'],axis=1,inplace=True)
    logger.info(cells_params_30D.columns)
    
    logger.info("prepared features")
    #cells_params_30D['group']= cells_params_30D['band'].apply(lambda x: x if x in ['T8','T4'] else 'D')
                    
    #cells_params_30D["tier"]= cells_params_30D.PRB_DL_AVAILABLE.apply(tiering) 
    
    
    group = cells_params_30D[cells_params_30D['band_letter']=='T']
    compare = group[['CELL_NAME','DATE_ID','DL_THROUGHPUT','DL_TRAFFIC_GB']]
    
    #for (grouping, tier), group in cells_params_30D.groupby(['group', 'tier']):
    logger.info(f"starting group {group.shape}")
    group['og_throughput']=group.DL_THROUGHPUT.values
    
    counter = 0
    while counter<=20 :  
        logger.info(f"starting group T iteration {counter}")
        group['DL_THROUGHPUT'] = group['og_throughput'] + (configuration['step']*(counter))
        #group['percentage']=(group['Dl_traffic']-group['DL_TRAFFIC_GB'])/group['DL_TRAFFIC_GB']
        logger.info(f"scaled throughpupt ")
        
        # transformed_df= stats_model(group[['Dl_traffic','DL_PRB_UTI','AVG_ACTIVE_UE_DL','UL_TRAFFIC_GB']],grouping,tier,configuration)
        
        # group['DL_PRB_UTI_t'] = transformed_df['DL_PRB_UTI_t'].values
        # group['AVG_ACTIVE_UE_DL_t'] = transformed_df['AVG_ACTIVE_UE_DL_t'].values
        # group['UL_TRAFFIC_GB_t'] = transformed_df['UL_TRAFFIC_GB_t'].values
        
        logger.info(f"group cells shape : {group.shape}") 

        logger.info("finished scaling")    
        output = inference(group,configuration)  
        
        logger.info(f"output columns  : {output.columns}") 
        logger.info(f"output shape  : {output.shape}") 
        logger.info("finished predicting")     
        compare[f'throughput_{counter}']=output['pred_traffic']
            # logger.info(f"output cells shape : {compare.shape}") 
            # if len(compare)!=0:
            #     logger.info("saving the output")    
            #     compare.to_parquet(os.path.join(configuration['output'],f"cells_{grouping}_{tier}_{counter}_new_iter.parquet"))
        
        counter +=1 
            
    compare.to_parquet(os.path.join(configuration['output'],f"cells_traffic_T_const_final.parquet"))    
        
        
        
    
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









    
    
    
    
    
    




