def data_prep_onboard(df):
    df.fillna('', inplace=True)
    #Removing Millisecs from datetime
    df['created_at']=df['created_at'].astype('datetime64[ns]') + timedelta(hours=5.5) 
    df['updated_at']=df['updated_at'].astype('datetime64[ns]') + timedelta(hours=5.5)    
    df['updated_at'] = df['updated_at'].astype('str')
    df['created_at'] = df['created_at'].astype('str')
    df['obj_id']=df['obj_id'].astype('str')
    df['retailagent_id']=df['retailagent_id'].astype('str')
    df['self_onboarding']=df['self_onboarding'].astype('str')
    df.drop('_id', axis=1, inplace=True)
    return df